package hadoop.demo;

import hadoop.HadoopNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by zk_chs on 16/8/5.
 */
public class WordCount {

    public static final IntWritable ONE = new IntWritable(1);
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String[] vs = value.toString().split("\\s");//正则表达式，表示通过空格分隔
            for (String v : vs) {
                context.write(new Text(v), ONE);
            }
        }
    }


    public static  class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) {

        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", HadoopNode.HDFS);
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            String[] paths = new GenericOptionsParser(conf, args).getRemainingArgs();
            if(paths.length < 2){
                throw new RuntimeException("usage <input> <output>");
            }

            Job job = Job.getInstance(conf, "wordcount");
            job.setJarByClass(WordCount.class);
            job.setCombinerClass(WordCountReducer.class); //有多个从机时需要指定reducer类，但是我这里是伪分布的只有一个所以不需要
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);  //因为map中返回的多了个long型的数据，在reduce接受的时候必须要转下字符类型
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(WordCountReducer.class);
            FileInputFormat.addInputPaths(job, paths[0]); //同时写入两个文件的内容
            FileOutputFormat.setOutputPath(job, new Path(paths[1])); //整合好结果后输出的位置
            System.exit(job.waitForCompletion(true) ? 0 : 1); //执行job
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
