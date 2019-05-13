import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

public class ZKEngines {
    public static void main(String... args) throws Exception, InterruptedException {
        Logger log = Logger.getLogger(ZKEngines.class);
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 15000, log::info);
        BasePathCreator basePathCreator = new BasePathCreator(zk, log);
        basePathCreator.createBasePath();
        Thread.sleep(60000);

    }
}
