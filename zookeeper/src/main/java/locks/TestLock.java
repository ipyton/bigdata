package locks;

import configurationCenter.DefaultWatch;
import configurationCenter.ZKConf;
import configurationCenter.ZKUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLock {

    ZooKeeper zk;
    ZKConf zkConf;
    DefaultWatch defaultWatch;


    @Before
    public void conn(){
        zkConf = new ZKConf();
        zkConf.setAddress("node3:2181,node4:2181,node5:2181/testLock");
        zkConf.setSessionTime(1000);
        defaultWatch = new DefaultWatch();
        ZKUtils.setConf(zkConf);
        ZKUtils.setWatch(defaultWatch);
        zk = ZKUtils.getZK();
    }

    @After
    public void close(){
        ZKUtils.closeZK();
    }


    @Test
    public void testLock(){
        for(int i = 0; i < 10; i ++) {
            new Thread(){
                public void run(){
                    WatchCallBack watchCallBack = new WatchCallBack();
                    watchCallBack.setZk(zk);
                    String name = Thread.currentThread().getName();
                    watchCallBack.setThreadName(name);

                    try {
                        //tryLock
                        watchCallBack.tryLock();
                        System.out.println(name + " at work");
                        watchCallBack.getRootData();
//                        Thread.sleep(1000);
                        //unLock
                        watchCallBack.unlock();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.run();
        }
        while (true) {



        }


    }


}
