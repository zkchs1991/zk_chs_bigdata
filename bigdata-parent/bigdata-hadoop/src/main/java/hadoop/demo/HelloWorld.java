package hadoop.demo;

import hadoop.HadoopNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zk_chs on 16/8/5.
 */
public class HelloWorld {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HadoopNode.HDFS);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HelloWorld.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumSumReducer.class);
        job.setReducerClass(IntSumSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/zk_in"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));
        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }

}
