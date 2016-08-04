package zookeeper;

import org.I0Itec.zkclient.ZkClient;

/**
 * Created by zk_chs on 16/8/3.
 */
public class ZkClientFactory {

    private static ZkClient zkClient = new ZkClient("10.0.11.141:2181,10.0.11.142:2181,10.0.11.143:2181");

    public static ZkClient getInstance (){
        return zkClient;
    }

}
