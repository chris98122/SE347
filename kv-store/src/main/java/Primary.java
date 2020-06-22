import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRouteException;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.exception.SofaTimeOutException;
import lib.PrimaryService;
import lib.WorkerService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    // startKey is Hash(workerAddr)
    // e.g -399182218 -->  212.64.64.185:12201

    TreeMap<String, ArrayList<String>> workerkeymap = new TreeMap<>();
    // stores workerAddr --> [KeyStart, KeyEnd] mapping
    // e.g 212.64.64.185:12201  --> [-399182218,1302869320]
    // KeyStart is Hash(workerAddr)

    volatile HashMap<String, ConsumerConfig<WorkerService>> workerConsumerConfigHashMap = new HashMap<String, ConsumerConfig<WorkerService>>();
    // stores workerAddr -->ConsumerConfig mapping
    // the ConsumerConfig may change to standby node due to worker failure

    volatile HashMap<String, WORKERSTATE> workerState = new HashMap<>();

    volatile List<String> workerlist = null;
    // 保存最新一次获取的workers
    CountDownLatch DataTransfertLatch = new CountDownLatch(0);

    CountDownLatch ScaleOutLatch = new CountDownLatch(0);

    CountDownLatch ProcessWorkerChangeLatch = new CountDownLatch(0);

    volatile HashMap<String, ReentrantReadWriteLock> keyRWLockMap = new HashMap<String, ReentrantReadWriteLock>();

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
                    2. new worker add :解决方法是new worker 60秒内都没有收到setKeyRange()就重新连接zookeeper
                    */
                    getWorkers();//watcher是一次性的所以必须再次注册
                    //客户端 Watcher 回调的过程是一个串行同步的过程，所以另开线程处理

                    // 一个时间只能有一个ProcessWorkerChange 线程
                    ProcessWorkerChangeLatch.await();

                    ProcessWorkerChange processWorkerChange = new ProcessWorkerChange();
                    processWorkerChange.setName("processWorkerChange" + ProcessWorkerChangeCounter.addAndGet(1));
                    processWorkerChange.setPriority(Thread.MAX_PRIORITY);
                    processWorkerChange.start();
                } catch (KeeperException | InterruptedException keeperException) {
                    keeperException.printStackTrace();
                }
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
                    LOG.info(String.valueOf(children));
                    break;
                default:
                    LOG.error("getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    Primary(String hostPort, String ip) throws UnknownHostException {
        this.hostPort = hostPort;
        serverId = ip;
        isLeader = false;
    }

    public static void main(String[] args)
            throws Exception {
        Primary m = new Primary(args[0], args[1]);
        m.startZK();
        m.runForPrimary();
        if (isLeader) {
            LOG.info("I'm the leader.");
            LOG.info("serverId:" + serverId);
            m.boostrap();

            while (m.workerState.size() < 2)// block until initialize at 2 workers
            {
                m.InitialhashWorkers();
            }

            m.getWorkers();//register the "/worker"" Watcher

            m.registerRPCServices(); //after setting the Master-Worker , the Master can receive rpc from clients
            m.run();
        } else {
            LOG.info("Some one else is the leader.");
        }
        //  m.stopZK();
    }

    public static Integer Hash(String string) {
        String encodeStr = DigestUtils.md5Hex(string);
        return encodeStr.hashCode();
    }

    @Override
    public String PUT(String key, String value) {
        String WorkerAddr = getWorkerADDR(key);
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerAddr);
            LOG.info("ASSIGN PUT " + key + ":" + value + " TO " + WorkerAddr);
            if (!workerState.get(WorkerAddr).equals(WORKERSTATE.READWRITE)) {
                //spin
                LOG.info(WorkerAddr + "can not put");
                return "the service is not available right now.";
            } else {
                ReentrantReadWriteLock lock;
                if (keyRWLockMap.containsKey(key)) {
                    lock = keyRWLockMap.get(key);
                } else {
                    lock = new ReentrantReadWriteLock();
                    keyRWLockMap.put(key, lock);
                }

                lock.writeLock().lock();
                LOG.info(" get lock of " + key);
                String res = null;
                int retrycounter = 0;
                while (retrycounter < 2) {
                    try {
                        res = workerService.PUT(key, value);
                        break;
                    } catch (SofaTimeOutException e) {
                        LOG.error(String.valueOf(e));
                    } catch (SofaRouteException e) {
                        LOG.error(String.valueOf(e));
                    } catch (SofaRpcException e) {
                        LOG.error(String.valueOf(e));
                    }
                    retrycounter++;
                    res = "ERR";
                }
                if (!lock.hasQueuedThreads()) {
                    lock.writeLock().unlock();
                    LOG.info(" keyRWLockMap.remove(key) ");
                    keyRWLockMap.remove(key);
                } else {
                    LOG.info("hasQueuedThreads");
                    lock.writeLock().unlock();
                }
                return res;
            }
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
            if (workerState.get(WorkerAddr).equals(WORKERSTATE.FAIL)) {
                LOG.info(WorkerAddr + "can not GET");
                return "the service is not available right now.";
            } else {
                ReentrantReadWriteLock lock;
                if (keyRWLockMap.containsKey(key)) {
                    lock = keyRWLockMap.get(key);
                } else {
                    lock = new ReentrantReadWriteLock();
                    keyRWLockMap.put(key, lock);
                }
                lock.readLock().lock();
                String res = null;
                int retrycounter = 0;
                while (retrycounter < 2) {
                    try {
                        res = workerService.GET(key);
                        break;
                    } catch (SofaTimeOutException e) {
                        LOG.error(String.valueOf(e));
                    } catch (SofaRouteException e) {
                        LOG.error(String.valueOf(e));
                    } catch (SofaRpcException e) {
                        LOG.error(String.valueOf(e));
                    }
                    retrycounter++;
                    res = "ERR";
                }
                if (!lock.hasQueuedThreads()) {
                    lock.readLock().unlock();
                    keyRWLockMap.remove(key);
                } else {
                    //LOG.info("hasQueuedThreads");
                    lock.readLock().unlock();
                }
                return res;
            }
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
            if (!workerState.get(WorkerAddr).equals(WORKERSTATE.READWRITE)) {
                //spin
                LOG.info(WorkerAddr + "can not delete");
                return "the service is not available right now.";
            } else {
                ReentrantReadWriteLock lock;
                if (keyRWLockMap.containsKey(key)) {
                    lock = keyRWLockMap.get(key);
                } else {
                    lock = new ReentrantReadWriteLock();
                    keyRWLockMap.put(key, lock);
                }
                lock.writeLock().lock();
                LOG.info(" get lock of " + key);
                String res = null;
                int retrycounter = 0;
                while (retrycounter < 2) {
                    try {
                        res = workerService.DELETE(key);
                        break;
                    } catch (SofaTimeOutException e) {
                        LOG.error(String.valueOf(e));
                    } catch (SofaRouteException e) {
                        LOG.error(String.valueOf(e));
                    } catch (SofaRpcException e) {
                        LOG.error(String.valueOf(e));
                    }
                    retrycounter++;
                    res = "ERR";
                }
                if (!lock.hasQueuedThreads()) {
                    lock.writeLock().unlock();
                    LOG.info(" keyRWLockMap.remove(key) ");
                    keyRWLockMap.remove(key);
                } else {
                    LOG.info("hasQueuedThreads");
                    lock.writeLock().unlock();
                }
                return res;
            }
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

    String resetKeyRange(String newKeyEnd, String WorkerSenderAddr, String WorkerReceiverADRR) {
        // block until the data transfer is done
        // reassign worker keyrange
        //reuse the interface of SetKeyRange
        try {
            WorkerService workerService = GetServiceByWorkerADDR(WorkerSenderAddr);
            LOG.info("resetKeyRange " + WorkerSenderAddr + " TO " + newKeyEnd);
            checkNewKeyEnd(newKeyEnd, WorkerSenderAddr);

            // send unhashed string ,e.g 127.0.0.1:12000
            String UnhashedOldKeyEnd = null;
            // use workermap to find  UnhashedOldKeyEnd
            // workermap stores startKey --> workerAddr mapping
            Iterator iterator = workerkeymap.keySet().iterator();
            String workeraddr = null;
            while (iterator.hasNext()) {
                workeraddr = (String) iterator.next();
                if (workerkeymap.get(WorkerSenderAddr).get(1).equals(Hash(workeraddr).toString())) {
                    UnhashedOldKeyEnd = workeraddr;
                    //the UnhashedOldKeyEnd must be some worker's addr
                    // so we just compare the Hashed value with KeyEnd
                    break;
                }
            }
            assert UnhashedOldKeyEnd != null;

            synchronized (workerState) {
                workerState.put(WorkerSenderAddr, WORKERSTATE.READONLY);
            }

            String res = workerService.ResetKeyEnd(UnhashedOldKeyEnd, WorkerReceiverADRR, WorkerReceiverADRR);
//            workerkeymap.get(WorkerSenderAddr).set(1, newKeyEnd);
//            synchronized (workerState) {
//                workerState.put(WorkerSenderAddr, WORKERSTATE.READWRITE);
//            }
            return res;
        } catch (Exception e) {
            LOG.error(String.valueOf(e));
            e.printStackTrace();
        }
        return "ERR";
    }

    public void getWorkers() throws KeeperException, InterruptedException {
        LOG.info("register workersChangeWatcher");
        zk.getChildren("/workers", workersChangeWatcher, workerGetChildrenCallback, null);
        // register the workersChangeWatcher
    }

    String getWorkerADDR(String keyString) {
        int hashvalue = Hash(keyString);
        for (String workerkey : workerkeymap.keySet()) {
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

        List<String> workerlist = zk.getChildren("/workers", false);
        while (workerlist.isEmpty() || workerlist.size() == 1) {
            Thread.sleep(600);
            LOG.warn("InitialhashWorkers(): workers not exist.");
            workerlist = zk.getChildren("/workers", false);
        }
        for (String w : workerlist) {
            workermap.put(Hash(w), w);
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
            for (String w : workerlist) {
                if (Hash(w).equals(keyStart)) {
                    // make worker's KeyStart is the Hash(workerAddr)
                    workerkeymap.put(w, list);
                    break;
                }
            }
            keyStart = keyEnd;
        }
        // send all key range to Worker
        iterator = workerkeymap.keySet().iterator();
        while (iterator.hasNext()) {
            String workerAddr = (String) iterator.next();
            WorkerService workerService = GetServiceByWorkerADDR(workerAddr);
            int retrycounter = 0;
            while (retrycounter < 10) {
                String res = null;
                try {
                    res = workerService.SetKeyRange(workerkeymap.get(workerAddr).get(0), workerkeymap.get(workerAddr).get(1), false);
                    LOG.info("[RPC RESPONSE]" + res);
                } catch (SofaRpcException e) {
                    LOG.error(String.valueOf(e));
                }
                if (res.equals("OK")) {
                    workerState.put(workerAddr, WORKERSTATE.READWRITE);// mark workers as active
                    break;
                } else {
                    retrycounter++;
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        LOG.error(String.valueOf(e));
                    }
                    LOG.info("InitialhashWorkers: retry SetKeyRange");
                }

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
                    .setTimeout(3000)//默认值3000
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
                .setDaemon(false)
                .setMaxThreads(40); // 非守护线程

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
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    @Override
    public String notifyTransferFinish(String WorkerSenderAddr, String newKeyEnd) {
//        workerkeymap.get(WorkerSenderAddr).set(1, newKeyEnd);
//        synchronized (workerState) {
//            workerState.put(WorkerSenderAddr, WORKERSTATE.READWRITE);
//        }
        LOG.info("GET notifyTransferFinish");
        while (DataTransfertLatch.getCount() != 1) {
            //spin
        }
        DataTransfertLatch.countDown();
        return "OK";
    }


    public enum WORKERSTATE {
        READWRITE, READONLY, FAIL
    }

    class ProcessWorkerChange extends Thread {
        public void run() {
            // 一个时间只能有一个ProcessWorkerChange 线程
            ProcessWorkerChangeLatch = new CountDownLatch(1);
            try {
                workerlist = zk.getChildren("/workers", null);//sync call
                LOG.info(String.valueOf(workerlist));
            } catch (KeeperException | InterruptedException keeperException) {
                keeperException.printStackTrace();
            }
            int newWorkerNum = 0;

            int oldWorkerNum = 0;

            for (String worker : workerlist) {
                if (workerState.containsKey(worker)) {
                    oldWorkerNum++;
                    String WorkerAddr = null;
                    Boolean retrying = true;
                    while (retrying) {
                        try {
                            WorkerAddr = new String(zk.getData("/workers/" + worker, false, null));
                            retrying = false;

                            synchronized (workerState) {
                                if (!workerState.get(worker).equals(WORKERSTATE.READONLY)) {
                                    workerState.put(worker, WORKERSTATE.READWRITE);
                                }
                            }
                        } catch (KeeperException.NoNodeException e) {
                            synchronized (workerState) {
                                workerState.put(worker, WORKERSTATE.FAIL);
                            }
                            LOG.info(worker + "znode 不存在,retry");
                        } catch (KeeperException | InterruptedException e) {
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    String oldaddr = workerConsumerConfigHashMap.get(worker).getDirectUrl().substring(7);
                    LOG.info(oldaddr);
                    LOG.info(WorkerAddr);
                    if (!WorkerAddr.equals(oldaddr)) {
                        synchronized (workerState) {
                            workerState.put(worker, WORKERSTATE.FAIL);
                        }
                        // change workerConsumerConfigHashMap to standby  node
                        // so that GetServiceByWorkerADDR get the standby node address
                        ConsumerConfig<WorkerService> consumerConfig;
                        String workerip = WorkerAddr.split(":")[0];
                        String workerport = WorkerAddr.split(":")[1];
                        consumerConfig = new ConsumerConfig<WorkerService>()
                                .setInterfaceId(WorkerService.class.getName()) // 指定接口
                                .setProtocol("bolt") // 指定协议
                                .setDirectUrl("bolt://" + workerip + ":" + workerport) // 指定直连地址
                                .setTimeout(2000)
                                .setRepeatedReferLimit(30); //允许同一interface，同一uniqueId，不同server情况refer 30次，用于单机调试

                        synchronized (workerConsumerConfigHashMap) {
                            workerConsumerConfigHashMap.put(worker, consumerConfig);
                        }
                        LOG.info("set ConsumerConfig  " + worker + "->" + WorkerAddr);
                        //set  workerState
                        synchronized (workerState) {
                            workerState.put(worker, WORKERSTATE.READWRITE);
                        }
                    }
                } else {
                    // ScaleOut needed
                    newWorkerNum++;
                }
            }
            if (workerState.size() - oldWorkerNum > 0) {
                LOG.warn(workerState.size() - oldWorkerNum + "worker failed");
            }
            if (newWorkerNum >= 1) {
                LOG.info(newWorkerNum + " new workers");

                try {
                    ScaleOutLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 事实上如果primary 繁忙，scaleout 线程几乎不会被调度到
                ScaleOut scaleOut = new ScaleOut();
                scaleOut.setName("scaleOut" + ScaleOutCounter.addAndGet(1));
                scaleOut.setPriority(Thread.MAX_PRIORITY);//例如processworker程启动scaleout线程，则scaleout的优先级应该也是10
                // 线程的优先级只是调度器的一个提示。
                // Thread类的setPriority()方法为线程设置了新的优先级。
                scaleOut.start();
            }
            ProcessWorkerChangeLatch.countDown();
        }
    }

    class ScaleOut extends Thread {
        /*
                        如果在注册后立马有新的workersChangeWatcher回调
                        需要保证ScaleOut线程一个时间只运行一个
                        而且对workerConsumerConfigHashMap等map的操作也保证是线程安全的
                    */
        public void run() {

            ScaleOutLatch = new CountDownLatch(1);
            LOG.info("ScaleOut triggered");
            synchronized (workerkeymap) {
                // workerkeymap 只有在扩容成功时才会有workerReceiver
                // 而且对workerkeymap加锁 不会阻塞 PUT GET DELETE

                try {
                    workerlist = zk.getChildren("/workers", null);
                    // 保证获得是最新的workerlist
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (String workerReceiver : workerlist) {
                    if (!workerkeymap.containsKey(workerReceiver)) {
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
                        String res = null;
                        int retrycounter = 0;
                        while (retrycounter < 5) {
                            try {
                                LOG.info("workerReiverService.SetKeyRange()");
                                res = workerReiverService.SetKeyRange(workerReceiverKeyStart, workerReceiverKeyEnd, true);
                                if (res.equals("OK"))
                                    break;
                                else {
                                    Thread.sleep(500);
                                    retrycounter++;
                                }
                            } catch (SofaRpcException e) {
                                retrycounter++;
                                LOG.info("retry workerReiverService.SetKeyRange()");
                                LOG.error(String.valueOf(e));
                            } catch (Exception e) {
                                LOG.error(String.valueOf(e));
                                e.printStackTrace();
                            }
                        }
                        if (res.equals("OK")) {
                            // notify the workerSender's to reset keyrange
                            // workerSender do datatransfer
                            // reset workerkeymap

                            String newKeyEnd = Hash(workerReceiver).toString();
                            res = resetKeyRange(newKeyEnd, workerSenderAddr, workerReceiver);
                            LOG.info("resetKeyRange " + res);
                            // if there are more new workers , have to wait til the former one is all set up,
                            // 缺点是可能会a->b->c传两遍数据

                            while (true) {
                                if (res.equals("OK")) {
                                    workerkeymap.get(workerSenderAddr).set(1, newKeyEnd);
                                    synchronized (workerState) {
                                        workerState.put(workerSenderAddr, WORKERSTATE.READWRITE);
                                    }

                                    LOG.info("add " + workerReceiver + "  workerkeymap and workerState");
                                    ArrayList<String> a = new ArrayList<>();
                                    a.add(workerReceiverKeyStart);
                                    a.add(workerReceiverKeyEnd);

                                    workermap.put(Hash(workerReceiver), workerReceiver);
                                    workerkeymap.put(workerReceiver, a);
                                    synchronized (workerState) {
                                        workerState.put(workerReceiver, WORKERSTATE.READWRITE);
                                    }
                                    LOG.info("ScaleOut " + workerReceiver + " Finished");
                                    break;
                                } else if (res.equals("WAIT")) {
                                    DataTransfertLatch = new CountDownLatch(1);
                                    LOG.info("wait for " + workerSenderAddr + " to notify datatransfer finish");
                                    try {
                                        DataTransfertLatch.await();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                        LOG.error(String.valueOf(e));
                                    }
                                } else {
                                    LOG.warn("resetKeyRange FAIL");
                                    break;
                                }
                            }
                        } else {
                            LOG.warn("workerReiverService.SetKeyRange FAIL");
                        }
                    }
                }
            }

            ScaleOutLatch.countDown();
        }
    }

}
