package com.xiaoyun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * Created by xiaoyunxiao on 15-3-24.
 */
public class HdfsTest {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);
        if (args.length != 4) {
            System.err.println("Usage HdfsTest:<dfs path> <local path> <match str> <single file lines>");
            return;
        }

        Path input = new Path(args[0]);
        FileStatus[] inputStatus = hdfs.listStatus(input);
        String match = args[2];
        int lines = Integer.parseInt(args[3]);

        FSDataInputStream in = null;
        Scanner scanner = null;
        FSDataOutputStream out = null;
        int count = 0;
        int fileNum = 0;

        for (FileStatus status : inputStatus) {
            if (status.isDir()) {
                continue;
            }
            in = hdfs.open(status.getPath());
            scanner = new Scanner(in);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                if (!line.contains(match)) {
                    continue;
                }
                count++;
                if (count == 1) {
                    fileNum++;
                    Path localFile=new Path(args[1] + File.separator + fileNum);
                    out = local.create(localFile);
                }
                out.write((line + "\n").getBytes());
                if (count==lines){
                    if (out!=null)out.close();
                    count=0;
                }
            }
            scanner.close();
            in.close();
        }
        if (out!=null)out.close();
    }
}
