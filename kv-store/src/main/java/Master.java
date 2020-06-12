import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import lib.KVService;
import lib.KVimplement;
import lib.Util;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.TreeMap;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    static String serverId;
    static Boolean isLeader;
    ZooKeeper zk;
    String hostPort;
    TreeMap<Integer, String> workermap = new TreeMap<>();

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
    AsyncCallback.StatCallback rehashcallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object o, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    LOG.info("lose connection to zookeeper");
                    break;
                case NONODE:
                    LOG.info("worker go down when assign keys");
                    break;
                case BADVERSION:
                    LOG.info("BADVERSION");
                    break;
                case OK:
                    LOG.info("set key range successfully");
                    break;
                default:
                    LOG.error("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }

    };

    Master(String hostPort) throws UnknownHostException {
        this.hostPort = hostPort;
        InetAddress addr = InetAddress.getLocalHost();
        serverId = addr.getHostAddress();
        isLeader = false;
    }

    public static void main(String args[])
            throws Exception {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            System.out.println("I'm the leader.");
            System.out.println("serverId:" + m.serverId);
            m.boostrap();
            //  m.hashWorkers();
            m.registerServices();
            m.run();
        } else {
            System.out.println("Some one else is the leader.");
        }
        //  m.stopZK();
    }

    Integer hashWorkers(String workerstring) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(workerstring);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }

    String getWorkerIP(String key) {
        return null;
    }

    void resetkeyrange() {

    }

    void hashWorkers() throws KeeperException, InterruptedException {
        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String workerstate = new String(data);
            if (workerstate.equals("UnHashed")) {
                workermap.put(hashWorkers(w), w);
            }
        }
        Iterator iterator = workermap.keySet().iterator();
        Integer keyStart = workermap.lastKey();
        Integer keyEnd = null;
        while (iterator.hasNext()) {
            keyEnd = (Integer) iterator.next();
            String path = "/workers/" + workermap.get(keyEnd);
            zk.setData(path, (keyStart.toString() + '/' + keyEnd.toString()).getBytes(), zk.exists(path, true).getVersion(), rehashcallback, null);
            keyStart = keyEnd;
        }

    }

    void run() throws InterruptedException {
        while (true) {
            try {
                Util.getWorkers(zk);
            } catch (Exception e) {
                System.out.println(e);
            }
            Thread.sleep(600);
        }
    }

    void registerServices() {

        ServerConfig serverConfig = new ServerConfig()
                .setProtocol("bolt") // 设置一个协议，默认bolt
                .setPort(12200) // 设置一个端口，默认12200
                .setDaemon(false); // 非守护线程

        ProviderConfig<KVService> providerConfig = new ProviderConfig<KVService>()
                .setInterfaceId(KVService.class.getName()) // 指定接口
                .setRef(new KVimplement()) // 指定实现
                .setServer(serverConfig); // 指定服务端

        providerConfig.export(); // 发布服务
    }

    void startZK() {
        try {
            zk = new ZooKeeper(hostPort, 15000, this);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }


    boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
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
            } catch (KeeperException.NoNodeException e) {
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

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

}
