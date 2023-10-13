package lock;

import org.apache.ivy.ant.FixDepsTask;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;


//in case of fierce competitions of locks, we use a circle dependency of watch.
public class WatchCallBack implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {
    ZooKeeper zk;
    String threadName;
    CountDownLatch cc = new CountDownLatch(1);
    String pathName;

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public CountDownLatch getCc() {
        return cc;
    }

    public void setCc(CountDownLatch cc) {
        this.cc = cc;
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    public void tryLock() {
        try {
            System.out.println(threadName + " create");
            zk.create("/lock", threadName.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, this, "trylock"); //String call back
            cc.await(); // block first
        } catch (InterruptedException e){
            e.printStackTrace();
        }

    }

    public void unlock(){
        try {
            zk.delete(pathName, -1);
            System.out.println(threadName + "over work");
        } catch (InterruptedException e){
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }



    // children callback
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        Collections.sort(children);
        int i = children.indexOf(pathName.substring(1));

        if (i == 0){ //there is no
            System.out.println(threadName + "i am first");
            try {
                zk.setData("/", threadName.getBytes(StandardCharsets.UTF_8), -1);
                cc.countDown();
            } catch (KeeperException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            }

        } else {
            zk.exists("/" + children.get(i - 1), this, this, "sdf"); //watch the lock before me
        }

    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        //
    }


    // StringCallback
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        if (name != null) {
            System.out.println((String) ctx +threadName + "create Node" + name);

            pathName = name;

            zk.getChildren("/", false, this, "sdf");
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                zk.getChildren("/", false, this, "sdf");
                break;
            case NodeDataChanged:
                break;
            case NodeChildrenChanged:
                break;
        }
    }
}
