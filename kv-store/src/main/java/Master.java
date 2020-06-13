import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import lib.MasterService;
import lib.WorkerService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master<PAIR> implements Watcher, MasterService {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    static String serverId;
    static Boolean isLeader;
    ZooKeeper zk;
    String hostPort;
    TreeMap<Integer, String> workermap = new TreeMap<>();// helper structure for calculating workerkeymap
    TreeMap<String, List<String>> workerkeymap = new TreeMap<>();
    TreeMap<String, String> workerstate = new TreeMap<>();
    HashMap<String, ConsumerConfig<WorkerService>> workerConsumerConfigHashMap = new HashMap<String, ConsumerConfig<WorkerService>>();

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    LOG.info("retry register to zookeeper " + serverId);
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
    AsyncCallback.ChildrenCallback workerGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    try {
                        getWorkers();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                case OK:
                    LOG.info("Successfully got a list of workers:" + children.size() + "workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }

    };
    //主节点等待从节点的变化（包括worker node fail 或者 增加）
    //ZooKeeper客户端也可以通过getData，getChildren和exist三个接口来向ZooKeeper服务器注册Watcher
    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeChildrenChanged) {
                assert ("/workers".equals(e.getPath()));
                try {
                    getWorkers();
                } catch (KeeperException keeperException) {
                    keeperException.printStackTrace();
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
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
            throws Exception, MWException {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            try {
                System.out.println("I'm the leader.");
                System.out.println("serverId:" + m.serverId);
                m.boostrap();
                m.InitialhashWorkers();
                m.getWorkers();//register the "/worker"" Watcher

                m.registerRPCServices(); //after setting the Master-Worker , the Master can receive rpc from clients
                m.run();
            } catch (MWException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        } else {
            System.out.println("Some one else is the leader.");
        }
        //  m.stopZK();
    }

    @Override
    public String PUT(String key, String value) {
        String WorkerAddr = getWorkerADDR(key);
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerAddr);
            LOG.info("ASSIGN PUT " + key + ":" + value + " TO" + WorkerAddr);
            return workerService.PUT(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    @Override
    public String GET(String key) {
        String WorkerAddr = getWorkerADDR(key);
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerAddr);
            LOG.info("ASSIGN GET " + key + " TO" + WorkerAddr);
            return workerService.GET(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    @Override
    public String DELETE(String key) {
        System.out.println("delete" + key);
        return "delete" + key;
    }

    void reassignAndSet(List<String> children) {
        // reassign worker keyrange
    }

    public void getWorkers() throws KeeperException, InterruptedException {
        zk.getChildren("/workers", workersChangeWatcher, workerGetChildrenCallback, null);
    }

    Integer Hash(String workerstring) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(workerstring);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }

    String getWorkerADDR(String keyString) {
        Integer hashvalue = Hash(keyString);
        Iterator iterator = workerkeymap.keySet().iterator();
        while (iterator.hasNext()) {
            String workerkey = (String) iterator.next();
            // System.out.println(key);
            Integer keyStart = Integer.valueOf(workerkeymap.get(workerkey).get(0));
            Integer keyEnd = Integer.valueOf(workerkeymap.get(workerkey).get(1));
            if (hashvalue >= keyStart && hashvalue < keyEnd) {
                return workerkey;
            } else if ((hashvalue >= keyEnd || hashvalue < keyStart) && keyStart > keyEnd) {
                return workerkey;
            }
        }
        return "ERR";
    }

    void resetkeyrange() {

    }

    void InitialhashWorkers() throws KeeperException, InterruptedException, MWException {
        //此函数只在启动master时运行一次
        //可扩展性与workerfail依靠对worker znode的Watcher函数

        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            //System.out.println(w);
            String workerstate = new String(data);
            if (workerstate.equals("UnHashed")) {// workerstate stored in znode seems useless, may delete this afterwards
                workermap.put(Hash(w), w);
            }
        }
        if (workermap.isEmpty()) {
            throw new MWException("workers not exist");
        }
        Iterator iterator = workermap.keySet().iterator();
        Integer keyStart = workermap.lastKey();
        Integer keyEnd = null;
        while (iterator.hasNext()) {
            keyEnd = (Integer) iterator.next();
            // 假设主节点从不fail，将所有workerhash保存在workerkeymap,
            List<String> list = new ArrayList();
            list.add(keyStart.toString());
            list.add(keyEnd.toString());
            workerkeymap.put(workermap.get(keyEnd), list);
            keyStart = keyEnd;
        }
        // send all key range to Worker
        iterator = workerkeymap.keySet().iterator();
        while (iterator.hasNext()) {
            String workerAddr = (String) iterator.next();
            WorkerService workerService = GetServiceByWorkerADDR(workerAddr);
            try {
                LOG.info("[RPC RESPONSE]" + workerService.SetKeyRange(workerkeymap.get(workerAddr).get(0), workerkeymap.get(workerAddr).get(1)));
            } catch (SofaRpcException e) {
                LOG.error(String.valueOf(e));
            }
        }
    }

    WorkerService GetServiceByWorkerADDR(String WorkerAddr) {
        ConsumerConfig<WorkerService> consumerConfig;
        if (workerConsumerConfigHashMap.get(WorkerAddr) == null) {
            String workerip = WorkerAddr.split(":")[0];
            String workerport = WorkerAddr.split(":")[1];
            consumerConfig = new ConsumerConfig<WorkerService>()
                    .setInterfaceId(WorkerService.class.getName()) // 指定接口
                    .setProtocol("bolt") // 指定协议
                    .setDirectUrl("bolt://" + workerip + ":" + workerport) // 指定直连地址
                    .setTimeout(2000);
            workerConsumerConfigHashMap.put(WorkerAddr, consumerConfig);
        } else {
            consumerConfig = workerConsumerConfigHashMap.get(WorkerAddr);
        }
        // 生成代理类
        WorkerService workerService = consumerConfig.refer();
        return workerService;
    }

    void run() throws InterruptedException {
        while (true) {

            Thread.sleep(600);
        }
    }

    void registerRPCServices() {

        ServerConfig serverConfig = new ServerConfig()
                .setProtocol("bolt") // 设置一个协议，默认bolt
                .setPort(12200) // 设置一个端口，默认12200
                .setDaemon(false); // 非守护线程

        ProviderConfig<MasterService> providerConfig = new ProviderConfig<MasterService>()
                .setInterfaceId(MasterService.class.getName()) // 指定接口
                .setRef(this) // 指定实现
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
