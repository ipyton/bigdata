package config;

import org.apache.calcite.sql.validate.CollectNamespace;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class WatchCallBack implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {
    ZooKeeper zk;
    MyConf conf;
    CountDownLatch cc = new CountDownLatch(1);

    public MyConf getConf(){
        return conf;
    }

    public void setConf(MyConf conf) {
        this.conf = conf;
    }

    public ZooKeeper getZK(){
        return zk;
    }

    public void setZK(ZooKeeper zk){
        this.zk = zk;
    }


    public void aWait(){
        zk.exists("/AppConf", this, this, "ABC"); //What is "ABC"
        try{
            cc.wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if (data != null) {
            String s = new String(data);
            conf.setConf(s);
            cc.countDown();
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (stat != null) zk.getData("/AppConf", this, this, "sdfs"); //an object when callback is called this param will be give.
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            // this is an asynchronous process and if the result back, content will be give into callback
            // watcher can just be used only once so it need to be registered again.
            case NodeCreated:
                zk.getData("/AppConf", this, this, "sdfs"); //if this node is changed will get this node
            case NodeDeleted:
                conf.setConf("");
                cc = new CountDownLatch(1);
                break;
            case NodeDataChanged:
                zk.getData("/AppConf", this,this, "sdfs");
                break;
            case NodeChildrenChanged:
                break;
        }
    }
}
