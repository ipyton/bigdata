package configurationCenter;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

public class DefaultWatch implements Watcher {
    CountDownLatch init;

    public CountDownLatch getInit() {
        return init;
    }

    public void setInit(CountDownLatch init) {
        this.init = init;
    }

    @Override
    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        switch (state) {
            case Disconnected:
                init = new CountDownLatch(1);
                break;
            case SyncConnected:
                init.countDown();
                break;
        }
    }
}


