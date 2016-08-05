package hadoop.instances;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 * 去重,delete_duplicate
 */
public class DeDup {

    private static final Path INPUT_PATH = new Path("input/dedup/*");
    private static final Path OUTPUT_PATH = new Path("output/dedup/");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty(HADOOP_USER_NAME, HADOOP_USER_NAME_VALUE);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS, DEFAULT_FS_VALUE);
        conf.set(HDFS_IMPL, HDFS_IMPL_VALUE);
        Job job = Job.getInstance(conf, "dedup");
        job.setJarByClass(DeDup.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Map extends Mapper<LongWritable,Text,Text,Text>{
        private static final Text empty = new Text("");
        public void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
            context.write(value, empty);
        }
    }

    private static class Reduce extends Reducer<Text,Text,Text,Text>{
        private static final Text empty = new Text("");
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
            context.write(key, empty);
        }
    }

}
