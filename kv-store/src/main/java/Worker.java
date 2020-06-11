import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final String WorkerID;
    ZooKeeper zk;
    String hostPort;
    String serverId;
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();//try agagin
                    break;
                case OK:
                    LOG.info("Registered successfully:" + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered:" + serverId);
                    break;
                default:
                    LOG.error("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    Worker(String hostPort, String WorkerID) throws UnknownHostException {
        this.WorkerID = WorkerID;
        this.hostPort = hostPort;
        InetAddress addr = InetAddress.getLocalHost();
        long seed = System.nanoTime();
        Random rand = new Random(seed);
        serverId = addr.getHostAddress() + '-' + WorkerID;
        // WorkerID is used to distinguish different worker processes on one machine
    }

    public static void main(String args[]) throws Exception {
        Worker w = new Worker(args[0], args[1]);
        w.startZK();
        w.register();
        while (true) {
            Thread.sleep(600);
        }

    }

    void startZK() {
        try {
            zk = new ZooKeeper(hostPort, 15000, this);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent e) {
        LOG.info(e.toString() + "," + hostPort);
    }

    void register() {
        zk.create("/workers/worker-" + serverId, "UnHashed".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }
}
