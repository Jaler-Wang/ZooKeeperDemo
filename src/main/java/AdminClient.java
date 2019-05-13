import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

public class AdminClient {
    private ZooKeeper zk;
    private Logger log;
    private String hostport;

    public AdminClient(String hostport){
        this.hostport = hostport;
        log = Logger.getLogger(AdminClient.class);
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostport, 15000, log::info);
    }

    public void listState() throws KeeperException, InterruptedException {
        try{
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            log.info("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NoNodeException e){
            log.info("No Master");
        }

        log.info("Workers:");
        for(String w : zk.getChildren("/workers", false)){
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            log.info("\t" + w + ": " + state);
        }

        log.info("Tasks:");
        for(String t: zk.getChildren("/tasks", false)){
           log.info("\t" + t);
        }
    }

    public static void main(String... args) throws Exception {
        AdminClient c = new AdminClient("127.0.0.1:2181");
        c.startZK();
        c.listState();
    }
}
