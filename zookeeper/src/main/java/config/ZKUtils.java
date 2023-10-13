package config;

import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ZKUtils {
    private static ZooKeeper zk;

    private static String address = "node3:2181,node4:2181,node5:2181/testLock";

    private static DefaultWatch watch = new DefaultWatch();

    private static CountDownLatch init = new CountDownLatch(1);

    public static ZooKeeper getZK(){
        try {
            zk = new ZooKeeper(address, 1000, watch);
            watch.setCc(init);// wait until sync connected
            init.await();
        } catch (Exception e){
            e.printStackTrace();
        }

        return zk;
    }
}
