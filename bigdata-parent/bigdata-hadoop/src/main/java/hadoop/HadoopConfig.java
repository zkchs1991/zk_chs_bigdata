package hadoop;

/**
 * Created by zk_chs on 16/7/31.
 */
public class HadoopConfig {

    public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    public static final String HADOOP_USER_NAME_VALUE = "root";

    public static final String DEFAULT_FS = "fs.defaultFS";
    public static final String DEFAULT_FS_VALUE = "hdfs://Master:9000";

    public static final String HDFS_IMPL = "fs.hdfs.impl";
    public static final String HDFS_IMPL_VALUE = "org.apache.hadoop.hdfs.DistributedFileSystem";

}
