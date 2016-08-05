package hadoop.api;

import hadoop.HadoopNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by zk_chs on 16/7/31.
 */
public class Exists {

    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            String filename = "test";
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", HadoopNode.HDFS);
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(new Path(filename))) {
                System.out.println("文件存在");
            } else {
                System.out.println("文件不存在");
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
