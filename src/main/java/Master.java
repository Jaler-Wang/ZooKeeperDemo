import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master {
    private static String serverId = Long.toString(new Random().nextLong());
    private boolean isLeader = false;
    private ZooKeeper zk;
    private Logger log;
    private String hostPort;

    public Master(String hostPort) throws IOException {
        this.log = Logger.getLogger(Master.class);
        this.hostPort = hostPort;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, log::info);
    }
    public void createMaster(){
        zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, this::masterCreateCallback, null);
    }
    private void masterCreateCallback(int rc, String path, Object ctx, String name){
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMasterAync();
                return;
            case OK:
                isLeader = true;
                break;
            default:
                isLeader = false;
        }
        log.info("I'm " + (isLeader ? "": "not ") + "the leader");
    }

    private void masterCheckCallback(int rc, String path, Object ctx, byte[] data,
                                     Stat stat){
        switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMasterAync();
                return;
            case NONODE:
                createMaster();
                return;
        }
    }

    private void checkMasterAync() {
        zk.getData("/master", false, this::masterCheckCallback, null);
    }

    public static void main(String... args) throws Exception {
        Master master = new Master("127.0.0.1:2181");
        master.startZK();
        master.createMaster();
        Thread.sleep(60000);
    }
}
