import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master {
    enum MasterStates {RUNNING, ELECTED, NOTELECTED}

    private static String serverId = Long.toString(new Random().nextLong());
    private ZooKeeper zk;
    private Logger log;
    private String hostPort;
    private MasterStates state;

    public Master(String hostPort) {
        this.log = Logger.getLogger(Master.class);
        this.hostPort = hostPort;
        state = MasterStates.NOTELECTED;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, log::info);
    }

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, this::masterCreateCallback, null);
    }

    private void masterCreateCallback(int rc, String path, Object ctx, String name) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case OK:
                state = MasterStates.ELECTED;
                break;
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();
                break;
            default:
                state = MasterStates.NOTELECTED;
                log.error("Something went wrong when running for master.", KeeperException.create(KeeperException.Code.get(rc), path));
        }
        log.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader");
    }

    private void masterCheckCallback(int rc, String path, Object ctx, byte[] data,
                                     Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case NONODE:
                runForMaster();
                return;
            case OK:
                if (serverId.equals(new String(data))) {
                    state = MasterStates.ELECTED;
                } else {
                    state = MasterStates.NOTELECTED;
                    masterExists();
                }
                break;
            default:
                    log.error("Error when reading data.", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    }

    private void checkMaster() {
        zk.getData("/master", false, this::masterCheckCallback, null);
    }

    public void masterExists() {
        zk.exists("/master", this::masterExistsWatcher, this::masterExistsCallback, null);
    }

    private void masterExistsCallback(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                break;
            case NONODE:
                runForMaster();
                break;
            default:
                checkMaster();
                break;
        }
    }

    private void masterExistsWatcher(WatchedEvent watchedEvent) {
        if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
            assert "/master".equals(watchedEvent.getPath());
            runForMaster();
        }
    }

    public static void main(String... args) throws Exception {
        Master master = new Master("127.0.0.1:2181");
        master.startZK();
        master.runForMaster();
        Thread.sleep(60000);
    }
}
