package hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zk_chs on 16/8/5.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

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
