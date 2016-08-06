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
 * 多表关联
 */
public class MTjoin {

    private static final Path INPUT_PATH = new Path("input/MTjoin/*");
    private static final Path OUTPUT_PATH = new Path("output/MTjoin/");
    private static int time = 0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty(HADOOP_USER_NAME, HADOOP_USER_NAME_VALUE);
        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS, DEFAULT_FS_VALUE);
        conf.set(HDFS_IMPL, HDFS_IMPL_VALUE);
        Job job = Job.getInstance(conf, "MTjoin");
        job.setJarByClass(MTjoin.class);

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
            String line = value.toString();// 每行文件
            String relationType = "";// 左右表标识

            // 输入文件首行，不处理
            if (line.contains("factoryname") || line.contains("addressId")) {
                return;
            }

            // 输入的一行预处理文本
            StringTokenizer itr = new StringTokenizer(line);
            String mapKey = "";
            String mapValue = "";

            /**
             * i>0时,表示表是factory,relationType=1
             * i=0时,表示表是address,relationType=2
             * 在while循环的最后使i++进行区分
             * */
            int i = 0;
            while (itr.hasMoreTokens()) {
                // 先读取一个单词
                String token = itr.nextToken();
                // 判断该地址ID就把存到"values[0]"
                if (token.charAt(0) >= '0' && token.charAt(0) <= '9') {
                    mapKey = token;
                    if (i > 0) {
                        relationType = "1";
                    } else {
                        relationType = "2";
                    }
                    continue;
                }
                // 存工厂名
                mapValue += token + " ";
                i++;
            }

            // 输出左右表
            context.write(new Text(mapKey), new Text(relationType + "+" + mapValue));
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 输出表头
            if (0 == time) {
                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }

            int factoryNum = 0;
            String[] factory = new String[10];
            int addressNum = 0;
            String[] address = new String[10];

            for (Object value : values) {
                String record = value.toString();
                int len = record.length();
                int i = 2;
                if (0 == len) {
                    continue;
                }

                // 取得左右表标识
                char relationType = record.charAt(0);

                // 左表
                if (relationType == '1') {
                    factory[factoryNum] = record.substring(i);
                    factoryNum++;
                }

                // 右表
                if (relationType == '2') {
                    address[addressNum] = record.substring(i);
                    addressNum++;
                }
            }

            // 求笛卡尔积
            if (0 != factoryNum && 0 != addressNum) {
                for (int m = 0; m < factoryNum; m++) {
                    for (int n = 0; n < addressNum; n++) {
                        // 输出结果
                        context.write(new Text(factory[m]), new Text(address[n]));
                    }
                }
            }

        }
    }

}
