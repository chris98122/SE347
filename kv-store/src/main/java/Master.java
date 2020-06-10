import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;
    static String serverId;
    static Boolean isLeader;

    Master(String hostPort) {
        this.hostPort = hostPort;
        long seed = System.nanoTime();
        Random rand = new Random( seed);
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
            byte data[] = zk.getData("/workers/" + w, false, null);
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
                // create master znode should use CreateMode.EPHEMERAL
                // so the znode would be deleted when the connection is lost
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

    void createParent(String path, byte[] data) {
        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
        // async call
        // CreateMode.PERSISTENT means the znode would be durable
        // OPEN_ACL_UNSAFE means all ACL can create/read/write/delete the znode
    }

    public void boostrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);//try agagin
                    break;
                case OK:
                    LOG.info("Parent created.");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered:" + path);
                    break;
                default:
                    LOG.error("somthing went wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String args[])
            throws Exception {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (isLeader) {
            System.out.println("I'm the leader.");
            System.out.println("serverId:" + serverId);
            m.boostrap();
            while (true) {
                try {
                    m.getWorkers();
                } catch (Exception e) {
                    System.out.println(e);
                }
                Thread.sleep(600);
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
