package hadoop.chapter3;

import hadoop.HadoopNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by zk_chs on 16/7/31.
 */
public class Create {

    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", HadoopNode.HDFS);
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            byte[] buff = "Hello world".getBytes(); // 要写入的内容
            String filename = "test"; //要写入的文件名
            FSDataOutputStream os = fs.create(new Path(filename));
            os.write(buff,0,buff.length);
            System.out.println("Create:"+ filename);
            os.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
