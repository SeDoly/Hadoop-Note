#Jar包处理
    对一个工程打包，若指定MainClass,则通过hadoop jar命令运行时，默认运行MainClass类。可不指定MainClass,通过hadoop jar命令运行时，
    在jar包后面指定运行类(绝对路径):
    *例*
    对当前工程打包，若指定MainClass为MatrixMultiply，则hadoop jar hadooptest.jar args...命令运行MatrixMultiply。这时要运行如
    Selection等其他mapred任务时要打为其他jar包，指定相应MainClass；
    而打包时不指定MainClass，要运行Selection，执行hadoop jar hadooptest.jar book.hadoop.mapred.Selection args...即可。

#关于Projection
    Projection中，若使用Reducer，即简单的将Key输出，可以达到去重的效果(一个Key对应多个值Iterable，最终输出一个Key),而不使用Reducer，
    setNumReduceTasks为0,则输出为Map输出，无去重效果。

#build错误
    build时在文件头出现非法字符:\ufeff以及需要interface,enum等，字符集有问题。估计原文件为UTF-16编码，而ubuntu下idea默认打开为UTF-8
    ,因此，先将文件转换为UTF-16，在转换为UTF-8即可。(idea右下角，先选择UTF-16-转换，然后选择UTF-8转换)。

#log查看
    自己写的mapreduce程序，由LogFactory.getLog获取Log对象，然后通过log.info等在mapreduce过程中输出的日志可以在localhost:50030
    jobtracker管理界面点击相应job->map/reduce attempt_task->log info查看；也可在本地logs/userlogs/目录下查看，不过集群里日志
    不一定存在当前节点；