package book.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ͳ��������ƪ�ĵ�������Ӣ�ĵ�����һ��������ͬʱ���ֵĴ���
 *
 * @author KING
 */
public class WordConcurrnce {
    private static int MAX_WINDOW = 20;//����ͬ�ֵ���󴰿ڴ�С
    private static String wordRegex = "([a-zA-Z]{1,})";//����ƥ������ĸ��ɵļ�Ӣ�ĵ���
    private static Pattern wordPattern = Pattern.compile(wordRegex);//����ʶ��Ӣ�ﵥ��(�����ַ�-)
    private static IntWritable one = new IntWritable(1);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job wordConcurrenceJob = new Job();
        wordConcurrenceJob.setJobName("wordConcurrenceJob");
        wordConcurrenceJob.setJarByClass(WordConcurrnce.class);
        wordConcurrenceJob.getConfiguration().setInt("window", Integer.parseInt(args[2]));

        wordConcurrenceJob.setMapperClass(WordConcurrenceMapper.class);
        wordConcurrenceJob.setMapOutputKeyClass(WordPair.class);
        wordConcurrenceJob.setMapOutputValueClass(IntWritable.class);

        wordConcurrenceJob.setReducerClass(WordConcurrenceReducer.class);
        wordConcurrenceJob.setOutputKeyClass(IntWritable.class);
        wordConcurrenceJob.setOutputValueClass(WordPair.class);

        wordConcurrenceJob.setInputFormatClass(WholeFileInputFormat.class);
        wordConcurrenceJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(wordConcurrenceJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordConcurrenceJob, new Path(args[1]));

        wordConcurrenceJob.waitForCompletion(true);
        System.out.println("finished!");
    }

    public static class WordConcurrenceMapper extends Mapper<Text, BytesWritable, WordPair, IntWritable> {
        private int windowSize;
        private Queue<String> windowQueue = new LinkedList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            windowSize = Math.min(context.getConfiguration().getInt("window", 2), MAX_WINDOW);
        }

        /**
         * �����λ�ĵ����ļ�����ֵΪ�ĵ��е����ݵ��ֽ���ʽ��
         */
        @Override
        public void map(Text docName, BytesWritable docContent, Context context) throws
                IOException, InterruptedException {
            Matcher matcher = wordPattern.matcher(new String(docContent.getBytes(), "UTF-8"));
            while (matcher.find()) {
                windowQueue.add(matcher.group());
                if (windowQueue.size() >= windowSize) {
                    //���ڶ����е�Ԫ��[q1,q2,q3...qn]����[(q1,q2),1],[(q1,q3),1],
                    //...[(q1,qn),1]��ȥ
                    Iterator<String> it = windowQueue.iterator();
                    String w1 = it.next();
                    while (it.hasNext()) {
                        String next = it.next();
                        context.write(new WordPair(w1, next), one);
                    }
                    windowQueue.remove();
                }
            }
            if (!(windowQueue.size() <= 1)) {
                Iterator<String> it = windowQueue.iterator();
                String w1 = it.next();
                while (it.hasNext()) {
                    context.write(new WordPair(w1, it.next()), one);
                }
            }
        }

    }

    public static class WordConcurrenceReducer extends Reducer<WordPair, IntWritable, IntWritable, WordPair> {
        @Override
        public void reduce(WordPair wordPair, Iterable<IntWritable> frequence, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : frequence) {
                sum += val.get();
            }
            context.write(new IntWritable(sum), wordPair);
        }
    }
}
