import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class BasePathCreator {
    ZooKeeper zk;
    Logger log;

    public BasePathCreator(ZooKeeper zk, Logger log) {
        this.zk = zk;
        this.log = log;
    }

    public void createBasePath() {
        createParent("/workers", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/status", new byte[0]);
    }

    private void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, this::createCallback, data);
    }

    private void createCallback(int rc, String path, Object ctx, String name) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                createParent(path, (byte[]) ctx);
                break;
            case OK:
                log.info("Parent created");
                break;
            case NODEEXISTS:
                log.info("Parent already registered: " + path);
                break;
            default:
                log.info("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
        }
    }
}
