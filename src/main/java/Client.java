import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Client {
    private ZooKeeper zk;
    private String hostPort;
    private Logger log;

    public Client(String hostPort){
        this.hostPort = hostPort;
        log = Logger.getLogger(Client.class);
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, log::info);
    }

    public String queueCommand(String command) throws Exception {
        while(true){
            String name = null;
            try {
                name = zk.create("/tasks/task-", command.getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException.NodeExistsException e) {
                throw new Exception(name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {
            }
        }
    }

    public static void main(String... args) throws Exception {
        Client c = new Client("127.0.0.1:2181");
        c.startZK();
        String name = c.queueCommand("hello let's go");
        System.out.println(name);
    }
}
