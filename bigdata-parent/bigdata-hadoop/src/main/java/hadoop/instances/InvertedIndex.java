package hadoop.instances;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import static hadoop.HadoopConfig.*;

/**
 * Created by zk_chs on 16/8/7.
 */
public class InvertedIndex {

    private static final Path INPUT_PATH = new Path("input/InvertedIndex/*");
    private static final Path OUTPUT_PATH = new Path("output/InvertedIndex/");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty(HADOOP_USER_NAME, HADOOP_USER_NAME_VALUE);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS, DEFAULT_FS_VALUE);
        conf.set(HDFS_IMPL, HDFS_IMPL_VALUE);
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private FileSplit split; // 存储Split对象
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // key值由单词和URL组成，如"MapReduce:file1.txt"
                keyInfo.set(itr.nextToken() + ":" + split.getPath().getName());
                // 词频初始化为1
                valueInfo.set("1");

                context.write(keyInfo, valueInfo);
            }
        }
    }

    private static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 统计词频
            int sum = 0;
            for (Text ignored : values) sum++;

            int splitIndex = key.toString().indexOf(":");
            // 重新设置value值由URL和词频组成
            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            // 重新设置key值为单词
            key.set(key.toString().substring(0, splitIndex));

            context.write(key, info);
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            String fileList = "";
            for (Text value : values) {
                fileList += value.toString() + ";";
            }
            result.set(fileList);
            context.write(key, result);
        }
    }

}
