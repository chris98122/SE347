import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import lib.PrimaryService;
import lib.WorkerService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Primary implements Watcher, PrimaryService {
    private static final Logger LOG = LoggerFactory.getLogger(Primary.class);
    static String serverId;
    static Boolean isLeader;
    ZooKeeper zk;
    String hostPort;

    AtomicInteger ProcessWorkerChangeCounter = new AtomicInteger(0);

    AtomicInteger ScaleOutCounter = new AtomicInteger(0);

    TreeMap<Integer, String> workermap = new TreeMap<>();
    // helper structure for calculating workerkeymap
    // stores startKey --> workerAddr mapping
    // e.g -399182218 -->  212.64.64.185:12201
    // NOT USED SINCE INITIAL HASH WORKERS, MAY CHANGE TO LOCAL VARIABLE

    TreeMap<String, ArrayList<String>> workerkeymap = new TreeMap<>();
    // stores workerAddr --> [KeyStart, KeyEnd] mapping
    // e.g 212.64.64.185:12201  --> [-399182218,1302869320]

    HashMap<String, ConsumerConfig<WorkerService>> workerConsumerConfigHashMap = new HashMap<String, ConsumerConfig<WorkerService>>();
    // stores workerAddr -->ConsumerConfig mapping
    HashMap<String, Boolean> workerState = new HashMap<>();

    List<String> workerlist = null;
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
                    LOG.error("something went wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    AsyncCallback.ChildrenCallback workerGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    try {
                        LOG.info("retry get workers");
                        getWorkers();
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                case OK:
                    LOG.info("Successfully got a list of workers:" + children.size() + "workers");
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
                LOG.info("workersChangeWatcher triggered");
                try {
                    /*如果在重新注册watcher的时间里发生了"/worker"的变化
                    1. worker fail ：之后讨论
                    2. new worker add :解决方法是new worker 60秒内都没有收到setKeyRange()就打个LOG然后等运维手动重启
                    */
                    getWorkers();//watcher是一次性的所以必须再次注册
                    //客户端 Watcher 回调的过程是一个串行同步的过程，所以另开线程处理
                    ProcessWorkerChange processWorkerChange = new ProcessWorkerChange();
                    processWorkerChange.setName("processWorkerChange" + ProcessWorkerChangeCounter.addAndGet(1));
                    processWorkerChange.start();
                } catch (KeeperException | InterruptedException keeperException) {
                    keeperException.printStackTrace();
                }
            }
        }

    };

    Primary(String hostPort) throws UnknownHostException {
        this.hostPort = hostPort;
        InetAddress addr = InetAddress.getLocalHost();
        serverId = addr.getHostAddress();
        isLeader = false;
    }

    public static void main(String[] args)
            throws Exception {
        Primary m = new Primary(args[0]);
        m.startZK();
        m.runForPrimary();
        if (isLeader) {
            System.out.println("I'm the leader.");
            System.out.println("serverId:" + serverId);
            m.boostrap();
            m.InitialhashWorkers();// block until initialize at 2 workers
            m.getWorkers();//register the "/worker"" Watcher

            m.registerRPCServices(); //after setting the Master-Worker , the Master can receive rpc from clients
            m.run();
        } else {
            System.out.println("Some one else is the leader.");
        }
        //  m.stopZK();
    }

    public static Integer Hash(String string) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(string);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }

    @Override
    public String PUT(String key, String value) {
        String WorkerAddr = getWorkerADDR(key);
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerAddr);
            LOG.info("ASSIGN PUT " + key + ":" + value + " TO " + WorkerAddr);
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
            LOG.info("ASSIGN GET " + key + " TO " + WorkerAddr);
            return workerService.GET(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    @Override
    public String DELETE(String key) {
        String WorkerAddr = getWorkerADDR(key);
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerAddr);
            LOG.info("ASSIGN delete " + key + " TO " + WorkerAddr);
            return workerService.DELETE(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    void checkNewKeyEnd(String newKeyEnd, String WorkerAddr) {
        int keyStart = Integer.parseInt(workerkeymap.get(WorkerAddr).get(0));
        int keyEnd = Integer.parseInt(workerkeymap.get(WorkerAddr).get(1));
        if (keyStart < keyEnd) {
            assert keyEnd > Integer.parseInt(newKeyEnd);
        } else {
            assert Integer.parseInt(newKeyEnd) < keyEnd || Integer.parseInt(newKeyEnd) > keyStart;
        }
    }

    String resetKeyRange(String newKeyEnd, String WorkerAddr, String WorkerReceiverADRR) {
        // block until the data transfer is done
        // reassign worker keyrange
        //reuse the interface of SetKeyRange
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerAddr);
            LOG.info("resetKeyRange " + WorkerAddr + " TO " + newKeyEnd);
            checkNewKeyEnd(newKeyEnd, WorkerAddr);
            workerService.ResetKeyEnd(newKeyEnd, WorkerReceiverADRR);
            workerkeymap.get(WorkerAddr).set(1, newKeyEnd);
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    public void getWorkers() throws KeeperException, InterruptedException {
        zk.getChildren("/workers", workersChangeWatcher, workerGetChildrenCallback, null);
        // register the workersChangeWatcher
    }

    String getWorkerADDR(String keyString) {
        int hashvalue = Hash(keyString);
        for (String workerkey : workerkeymap.keySet()) {
            // System.out.println(key);
            int keyStart = Integer.parseInt(workerkeymap.get(workerkey).get(0));
            int keyEnd = Integer.parseInt(workerkeymap.get(workerkey).get(1));
            if (hashvalue >= keyStart && hashvalue < keyEnd) {
                {
                    LOG.info(hashvalue + " >= " + keyStart + " and < " + keyEnd);
                    return workerkey;
                }
            } else if ((hashvalue < keyEnd | hashvalue >= keyStart) && keyStart > keyEnd) {
                {
                    LOG.info(hashvalue + " >= " + keyStart + " or < " + keyEnd);
                    return workerkey;
                }
            }
        }
        return "ERR";
    }

    void InitialhashWorkers() throws KeeperException, InterruptedException {
        //此函数只在启动master时运行一次
        //可扩展性与workerfail依靠对worker znode的Watcher函数

        System.out.println("Workers:");
        List<String> workerlist = zk.getChildren("/workers", false);
        while (workerlist.isEmpty() || workerlist.size() == 1) {
            Thread.sleep(60);
            LOG.warn("InitialhashWorkers(): workers not exist.");
            workerlist = zk.getChildren("/workers", false);
        }
        for (String w : workerlist) {
            //System.out.println(w);
            workermap.put(Hash(w), w);
            workerState.put(w, true);// mark workers as active
        }
        Iterator iterator = workermap.keySet().iterator();
        Integer keyStart = workermap.lastKey();
        Integer keyEnd = null;
        while (iterator.hasNext()) {
            keyEnd = (Integer) iterator.next();
            // 假设主节点从不fail，将所有workerhash保存在workerkeymap,
            ArrayList<String> list = new ArrayList<>();
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
                LOG.info("[RPC RESPONSE]" + workerService.SetKeyRange(workerkeymap.get(workerAddr).get(0), workerkeymap.get(workerAddr).get(1), false));
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
                    .setTimeout(2000)
                    .setRepeatedReferLimit(30); //允许同一interface，同一uniqueId，不同server情况refer 30次，用于单机调试

            workerConsumerConfigHashMap.put(WorkerAddr, consumerConfig);
        } else {
            consumerConfig = workerConsumerConfigHashMap.get(WorkerAddr);
        }
        // 生成代理类
        return consumerConfig.refer();
    }

    void run() throws InterruptedException {
        while (true) {
            Thread.sleep(60);
        }
    }

    void registerRPCServices() {

        ServerConfig serverConfig = new ServerConfig()
                .setProtocol("bolt") // 设置一个协议，默认bolt
                .setPort(12200) // 设置一个端口，默认12200
                .setDaemon(false); // 非守护线程

        ProviderConfig<PrimaryService> providerConfig = new ProviderConfig<PrimaryService>()
                .setInterfaceId(PrimaryService.class.getName()) // 指定接口
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

    boolean checkPrimary() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/primary", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                return false;
            } catch (KeeperException.ConnectionLossException ignored) {
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    void runForPrimary() throws InterruptedException {
        while (true) {
            try {
                zk.create("/primary", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                // create master znode should use CreateMode.EPHEMERAL
                // so the znode would be deleted when the connection is lost
                isLeader = true;
                break;
            } catch (KeeperException.NoNodeException e) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException ignored) {
            } catch (KeeperException e) {
                e.printStackTrace();
            }
            if (checkPrimary()) break;
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

    // last get workerlist
    class ProcessWorkerChange extends Thread {
        public void run() {

            try {
                workerlist = zk.getChildren("/workers", null);//sync call
                LOG.info(String.valueOf(workerlist));
            } catch (KeeperException | InterruptedException keeperException) {
                keeperException.printStackTrace();
            }
            int newWorkerNum = 0;
            // check if worker fail
            for (String worker : workerlist) {
                if (workerState.containsKey(worker)) {
                    workerState.put(worker, false);// flip the state
                } else {
                    // ScaleOut needed
                    newWorkerNum++;
                }
            }
            for (Map.Entry<String, Boolean> entry : workerState.entrySet()) {
                if (!entry.getValue()) {
                    //flip back;
                    workerState.put(entry.getKey(), true);
                } else {
                    LOG.warn("worker " + entry.getKey() + " failed");
                }
            }
            if (newWorkerNum >= 1) {
                LOG.info(newWorkerNum + " new workers");
                //有可能newWorkerNum大于1，暂时先只处理等于1的情况
                ScaleOut scaleOut = new ScaleOut();
                scaleOut.setName("scaleOut" + ScaleOutCounter.addAndGet(1));
                scaleOut.start();
            }
        }

    }

    class ScaleOut extends Thread {       /*
                        如果在注册后立马有新的workersChangeWatcher回调
                        需要保证ScaleOut线程一个时间只运行一个
                        而且对workerConsumerConfigHashMap等map的操作也保证是线程安全的
                    */
        public void run() {
            LOG.info("ScaleOut triggered");
            synchronized (workerConsumerConfigHashMap) {
                // 设置workerConsumerConfigHashMap是线程安全的
                for (String workerReceiver : workerlist) {
                    if (!workerConsumerConfigHashMap.containsKey(workerReceiver)) {
                        //workerConsumerConfigHashMap add workerReceiver in GetServiceByWorkerADDR()
                        WorkerService workerReiverService = GetServiceByWorkerADDR(workerReceiver);
                        // it's a new worker
                        // find the worker just like the way put(key,value)  did
                        String workerSenderAddr = getWorkerADDR(workerReceiver);

                        // tell the workerReceiver to register as RPC Server
                        // (RPCport for data treansfer is 200+WorkerPort)
                        // reuse the SetKeyRange interface
                        String workerReceiverKeyStart = Hash(workerReceiver).toString();
                        String workerReceiverKeyEnd = workerkeymap.get(workerSenderAddr).get(1);
                        try {
                            workerReiverService.SetKeyRange(workerReceiverKeyStart, workerReceiverKeyEnd, true);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        // notify the workerSender's to reset keyrange
                        // workerSender do datatransfer
                        // reset workerkeymap
                        String newKeyEnd = Hash(workerReceiver).toString();
                        String res = resetKeyRange(newKeyEnd, workerSenderAddr, workerReceiver);
                        LOG.info("resetKeyRange " + res);
                        // if there are more new workers , have to wait til the former one is all set up,
                        // 缺点是可能会a->b->c传两遍数据

                        LOG.info("add" + workerReceiver + "into workerkeymap and workerState");
                        ArrayList<String> a = new ArrayList<>();
                        a.add(workerReceiverKeyStart);
                        a.add(workerReceiverKeyEnd);
                        workerkeymap.put(workerReceiver, a);
                        workerState.put(workerReceiver, true);
                    }
                }
            }
        }
    }

}
