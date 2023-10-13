package configurationCenter;

import config.MyConf;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class WatchCallBack implements Watcher, AsyncCallback.DataCallback, AsyncCallback.StatCallback {

    private ZooKeeper zk;
    private String watchPath;
    private CountDownLatch init;
    private MyConf confMsg;

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public String getWatchPath() {
        return watchPath;
    }

    public void setWatchPath(String watchPath) {
        this.watchPath = watchPath;
    }

    public CountDownLatch getInit() {
        return init;
    }

    public void setInit(int init) {
        System.out.println("setInit");
    }

    public MyConf getConfMsg() {
        return confMsg;
    }

    public void setConfMsg(MyConf confMsg) {
        this.confMsg = confMsg;
    }

    public void aWait(){
        try {
            zk.exists(watchPath, this,this, "initExists"); //ctx is used to indicate which place is register the callback
            init.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




    //data callback
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if (data != null) {
            confMsg.setConf(new String(data));
            init.countDown();
        }
    }


    //stat call back
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (stat != null) {
            try {
                zk.getData(watchPath, this, this, "ex");
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event.toString());
        Watcher.Event.EventType type = event.getType();
        switch (type){
            case NodeCreated:
                System.out.println("...watch@created");

            case NodeDeleted:
                try {
                    confMsg.setConf("");
                    init = new CountDownLatch(1);
                } catch (Exception e){
                    e.printStackTrace();
                }
                break;
            case NodeDataChanged:
                zk.getData(watchPath, this, this, "NodeChanged");
                break;
            case NodeChildrenChanged:
                break;
        }

    }




}
