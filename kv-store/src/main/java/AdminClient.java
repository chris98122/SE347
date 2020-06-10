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

    public void getWorkers() throws KeeperException, InterruptedException {
        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ":" + state);
        }
        ;
    }

    public void getTasks() throws KeeperException, InterruptedException {
        System.out.println("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
        ;
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    void listState() throws KeeperException, InterruptedException {
        getWorkers();
        getTasks();
    }
}

