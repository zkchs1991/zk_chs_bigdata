package hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zk_chs on 16/8/5.
 */
public class IntSumSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
