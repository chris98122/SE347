import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort;
    static String serverId;
    static Boolean isLeader;

    Master(String hostPort) {
        this.hostPort = hostPort;
        Random rand = new Random(25);
        serverId = Integer.toString(rand.nextInt() & Integer.MAX_VALUE);
        isLeader = false;
    }

    void startZK() {
        try {
            zk = new ZooKeeper(hostPort, 15000, this);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public void getWorkers() throws KeeperException, InterruptedException {
        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ":" + state);
        }
    }

    public void getTasks() throws KeeperException, InterruptedException {

        System.out.println("Tasks:");
        try {
            for (String t : zk.getChildren("/assign", false)) {
                System.out.println("\t" + t);
            }
        } catch (KeeperException.NodeExistsException e) {

        }
    }

    boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NodeExistsException e) {
                return false;
            } catch (KeeperException.ConnectionLossException e) {
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e) {
            } catch (KeeperException e) {
                e.printStackTrace();
            }
            if (checkMaster()) break;
        }
    }

    public static void main(String args[])
            throws Exception {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (isLeader) {
            System.out.println("I'm the leader.");
            System.out.println("serverId:" + serverId);
            while (true) {
                try {
                    m.getWorkers();
                } catch (Exception e) {
                    System.out.println(e);
                }
                Thread.sleep(60000);
            }
        } else {
            System.out.println("Some one else is the leader.");
        }
        //  m.stopZK();

    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

}
