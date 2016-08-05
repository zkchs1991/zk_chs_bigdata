package hadoop.instances;

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

import static hadoop.HadoopConfig.*;

/**
 * Created by zk_chs on 16/8/5.
 */
public class Sort {

    private static final Path INPUT_PATH = new Path("input/sort/*");
    private static final Path OUTPUT_PATH = new Path("output/sort/");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty(HADOOP_USER_NAME, HADOOP_USER_NAME_VALUE);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS, DEFAULT_FS_VALUE);
        conf.set(HDFS_IMPL, HDFS_IMPL_VALUE);
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Sort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable data = new IntWritable();
        private IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            data.set(Integer.parseInt(value.toString()));
            context.write(data, one);
        }
    }

    private static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable lineNum = new IntWritable(1);
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values){
                context.write(lineNum, key);
                lineNum = new IntWritable(lineNum.get() + 1);
            }
        }
    }

}
