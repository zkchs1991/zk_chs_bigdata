package hadoop.chapter3;

import hadoop.HadoopNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

/**
 * Created by zk_chs on 16/7/31.
 */
public class Merge {

    private Path inputPath = null;
    private Path outputPath = null;

    private Merge(String input, String output){
        System.setProperty("HADOOP_USER_NAME", "root");
        this.inputPath = new Path(input);
        this.outputPath = new Path(output);
    }

    private void doMerge() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HadoopNode.HDFS_ZK_CHS);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fsSource = FileSystem.get(URI.create(inputPath.toString()), conf);
        FileSystem fsDst = FileSystem.get(URI.create(outputPath.toString()), conf);
        FileStatus[] sourceStatus = fsSource.listStatus(inputPath, new MyPathFilter(".*\\.abc"));
        FSDataOutputStream fsdos = fsDst.create(outputPath);
        for (FileStatus sta : sourceStatus){
            System.out.print("路径: " + sta.getPath() + "     文件大小: " + sta.getLen()
                            + "     权限: " + sta.getPermission() + "内容: ");
            FSDataInputStream fsdis = fsSource.open(sta.getPath());
            byte[] data = new byte[1024];
            int read;
            PrintStream ps = new PrintStream(System.out);
            while ((read = fsdis.read(data)) > 0){
                ps.write(data, 0, read);
                fsdos.write(data, 0, read);
            }
            fsdis.close();
            ps.close();
        }
        fsdos.close();
    }

    public static void main(String[] args) throws IOException {
        Merge merge = new Merge("hdfs://192.168.0.177:9000/user/root/input/",
                                "hdfs://192.168.0.177:9000/user/root/tempwork/merge.txt");
        merge.doMerge();
    }

}

class MyPathFilter implements PathFilter{
    private String reg = null;
    MyPathFilter (String reg){
        this.reg = reg;
    }
    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(reg);
    }
}
