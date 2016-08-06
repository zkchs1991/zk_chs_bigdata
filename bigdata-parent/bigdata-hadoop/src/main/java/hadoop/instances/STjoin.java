package hadoop.instances;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import static hadoop.HadoopConfig.*;

/**
 * Created by zk_chs on 16/8/6.
 */
public class STjoin {

    private static final Path INPUT_PATH = new Path("input/STjoin/*");
    private static final Path OUTPUT_PATH = new Path("output/STjoin/");
    private static int time = 0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty(HADOOP_USER_NAME, HADOOP_USER_NAME_VALUE);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS, DEFAULT_FS_VALUE);
        conf.set(HDFS_IMPL, HDFS_IMPL_VALUE);
        Job job = Job.getInstance(conf, "STjoin");
        job.setJarByClass(STjoin.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Map extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String childName;
            String parentName;
            String relationType; // 左右表标识

            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] values = new String[2];
            int i = 0;
            while (itr.hasMoreTokens()){
                values[i] = itr.nextToken();
                i++;
            }

            if (values[0].compareTo("child") != 0){
                childName = values[0];
                parentName = values[1];

                // 输出左表
                relationType = "1";
                context.write(new Text(values[1]), new Text(relationType + "+" + childName + "+" + parentName));

                // 输出右表
                relationType = "2";
                context.write(new Text(values[0]), new Text(relationType + "+" + childName + "+" + parentName));
            }

        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 输出表头
            if (time == 0){
                context.write(new Text("grandChild"), new Text("grandParent"));
                time++;
            }

            /** 真实项目中,这里可以使用list取代数组 */
            int grandchildNum = 0;
            String[] grandchild = new String[10];
            int grandparentNum = 0;
            String[] grandparent = new String[10];

            for (Text value : values) {
                String record = value.toString();
                int len = record.length();
                int i = 2;
                if (len == 0) {
                    continue;
                }

                // 取得左右表标识
                char relationType = record.charAt(0);
                // 定义孩子和父母变量
                String childName = "";
                String parentName = "";

                // 获得value-list中value的child
                while (record.charAt(i) != '+') {
                    childName += record.charAt(i);
                    i++;
                }

                i = i + 1;

                // 获得value-list中value的parent
                while (i < len) {
                    parentName += record.charAt(i);
                    i++;
                }

                // 左表,取出child放入grandchild
                if (relationType == '1') {
                    grandchild[grandchildNum] = childName;
                    grandchildNum++;
                }

                // 右表,取出parent放入grandparent
                if (relationType == '2') {
                    grandparent[grandparentNum] = parentName;
                    grandparentNum++;
                }
            }

            // grandchild和grandparent数组求笛卡尔积
            if (grandchildNum != 0 && grandparentNum != 0) {
                for (int m = 0; m < grandchildNum; m++) {
                    for (int n = 0; n < grandparentNum; n++) {
                        // 输出结果
                        context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                    }
                }
            }

        }
    }

}
