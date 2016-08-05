package hadoop.demo;

import hadoop.HadoopNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

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
        job.setJarByClass(HelloWorld.class); //设定作业的启动类
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumSumReducer.class);
        job.setReducerClass(IntSumSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));
        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }

}

class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1); //注意这里实列化时要付个1，否则后面的计数会不准确
    private Text word = new Text();
    public void map (Object key, Text value,
                     Context context)
            throws IOException, InterruptedException {
        System.out.println("key = " + key.toString()); //添加查看的key值
        System.out.println("value = " + value.toString());

        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()){
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

}

class IntSumSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce (Text key, Iterable<IntWritable> values,
                        Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        System.out.println("reduces's key ======== " + key);
        for (IntWritable val : values){
            sum += val.get();
            System.out.println("reduces's val ======== " + val.toString());
        }
        result.set(sum);
        context.write(key, result);
    }

}
