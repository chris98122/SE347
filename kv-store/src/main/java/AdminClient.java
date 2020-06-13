import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class AdminClient implements Watcher {
    ZooKeeper zk;
    String hostPort;

    AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    public static void main(String args[]) throws Exception {
        AdminClient adminClient = new AdminClient(args[0]);
        adminClient.start();

        adminClient.listState();

    }

    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    void listState() throws KeeperException, InterruptedException {

    }
}

