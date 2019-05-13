import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class Worker {
    private ZooKeeper zk;
    private Logger log;
    private String hostPort;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private String status;

    public Worker(String hostPort) {
        this.log = Logger.getLogger(Worker.class);
        this.hostPort = hostPort;
    }

    private void startZK() throws IOException {
        this.zk = new ZooKeeper(hostPort, 15000, log::info);
    }

    public void register(){
        zk.create("/workers/worker-" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                this::createWorkerCallback, null);
    }

    private void createWorkerCallback(int rc, String path, Object ctx, String name) {
        switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                log.info("Registered successfully: " + serverId);
                break;
            case NODEEXISTS:
                log.warn("Already registered: " + serverId);
                break;
                default:
                    log.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
        }
    }

    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }

    private synchronized void updateStatus(String status) {
        if(status.equals(this.status)){
            zk.setData("/workers/" + serverId, status.getBytes(), -1, this::setStatusCallback, status);
        }
    }

    private void setStatusCallback(int rc, String path, Object ctx, Stat stat) {
        switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                updateStatus((String)ctx);
                return;
        }
    }

    public static void main(String... args) throws Exception {
        Worker w = new Worker("127.0.0.1:2181");
        w.startZK();
        w.register();
        Thread.sleep(60000);
    }
}
