package locks;

import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColLessEqualCharScalar;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WatchCallBack implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback, AsyncCallback.DataCallback{
    ZooKeeper zk;
    CountDownLatch cc = new CountDownLatch(1);
    String lockName ;
    String threadName;


    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public CountDownLatch getCc() {
        return cc;
    }

    public void setCc(CountDownLatch cc) {
        this.cc = cc;
    }

    public String getLockName() {
        return lockName;
    }

    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public void tryLock(){
        try {
            zk.create("/lock", threadName.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL,this, threadName);
            cc.await();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }


    public void unlock() {
        try {
            zk.delete("/" + lockName, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    public void getRootData() throws KeeperException, InterruptedException {
        byte[] data = zk.getData("/", false, new Stat());
        System.out.println(new String(data));
    }


    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        if (children == null) {
            System.out.println(ctx.toString() + "list null");
        } else {
            try {
                Collections.sort(children);
                int i = children.indexOf(lockName);
                if(i<1){
                    System.out.println(threadName+" i am first...");
                    zk.setData("/",threadName.getBytes(),-1);
                    cc.countDown();
                }else{
                    System.out.println(threadName+" watch "+children.get(i-1));
                    zk.exists("/"+children.get(i-1),this);
                }
            } catch (Exception e) {
                e.printStackTrace();

            }
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {

    }

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("create path" + name);
        lockName = name.substring(1);
        zk.getChildren("/", false, this, ctx);
    }

    @Override
    public void process(WatchedEvent event) {
        Event.EventType type = event.getType();

        switch (type) {
            case NodeDeleted:
                zk.getChildren("/", false, this, "");
                break;
            case NodeChildrenChanged:
                break;
        }
    }




}
