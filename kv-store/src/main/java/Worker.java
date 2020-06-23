import DB.RingoDB;
import DB.RingoDBException;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRouteException;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.exception.SofaTimeOutException;
import lib.DataTransferService;
import lib.PrimaryService;
import lib.WorkerService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Worker implements Watcher, WorkerService, DataTransferService {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    // primary data node metadata
    private final String primaryNodeIP;
    private final String primaryNodePort;
    private final String primaryNodeAddr;
    // real node metadata
    private final String realAddress;
    private final String realIP;
    private final String realPort;
    private final String zookeeperaddress;
    volatile boolean isRecover;
    ZooKeeper zk;
    volatile HashMap<String, ReentrantReadWriteLock> keyRWLockMap = new HashMap<String, ReentrantReadWriteLock>();
    volatile private Set<String> StandBySet = new HashSet<String>();
    volatile private String KeyStart = null;
    private String KeyEnd = null;
    // stores workerAddr -->ConsumerConfig mapping
    volatile private HashMap<String, ConsumerConfig<WorkerService>> workerConsumerConfigHashMap = new HashMap<String, ConsumerConfig<WorkerService>>();
    volatile private boolean isPrimary;
    Watcher workerExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            LOG.info("workerExistsWatcher" + e.getPath());
            registerWorkerWatcher();//watcher是一次性的所以必须再次注册
            if (e.getType() == Event.EventType.NodeDeleted) {
                assert ("/workers/" + primaryNodeAddr).equals(e.getPath());
                //如果是自己所属的worker断开连接,则尝试自己成为PrimaryDtaNode
                runForPrimaryDataNode();
            }
        }
    };
    AsyncCallback.StatCallback workerExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    // 连接丢失的情况下重试
                    LOG.info("CONNECTIONLOSS , retry PrimaryDataNodeExists()");
                    PrimaryDataNodeExists();
                    break;
                case OK:
                    //OK就说明存在znode,暂时不做什么
                    break;
                case NONODE:
                    //判断znode节点是否存在，不存在就竞选runForPrimaryDataNode
                    runForPrimaryDataNode();
                    break;
                default:
                    // 如果发生意外情况，通过获取节点数据来检查/workers/+primaryNodeAddr 节点是否存在
                    LOG.info("sth is wrong, checkPrimaryDataNode()");
                    checkPrimaryDataNode();
                    break;
            }
        }
    };

    Worker(String zookeeperaddress, String primarynode, String realnode, String isrecover) throws UnknownHostException {
        this.isRecover = isrecover.equals("isrecover");

        String primaryNodeIP = primarynode.split(":")[0];
        String primaryNodePort = primarynode.split(":")[1];

        String realIP = realnode.split(":")[0];
        String realPort = realnode.split(":")[1];

        this.primaryNodeIP = primaryNodeIP;
        this.primaryNodePort = primaryNodePort;
        this.zookeeperaddress = zookeeperaddress;
        primaryNodeAddr = primaryNodeIP + ':' + primaryNodePort;
        // serverId is the workerAddr that is used for hashing
        // which is the primary data node address
        this.realIP = realIP;
        this.realPort = realPort;
        this.realAddress = realIP + ":" + realPort;
        // realAddress is the address worker actually runs on
        LOG.info("WORKER METADATA:" + " [primaryNodeAddr] " + primaryNodeAddr + " [realAddress] " + realAddress);
        if (this.isRecover)
            LOG.info("is gonna recover");
    }

    public static void DoSnapshot() {
        int snapshotcounter = 0;

        while (true) {
            try {
                TimeUnit.MINUTES.sleep(1);//一分钟snapshot一次
                RingoDB.INSTANCE.snapshot();//保存最新的2次snapshot
                snapshotcounter++;
                if (snapshotcounter >= 3) {
                    // 删除snapshot
                    RingoDB.INSTANCE.delete_oldest_snapshot();
                }
            } catch (InterruptedException | IOException e) {
                LOG.error(String.valueOf(e));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(args[0], args[1], args[2], args[3]);
        w.startZK();
        w.registerRPCServices();// make sure the RPC can work, then register to zookeeper

        //保证Primary worker先抢占到领导权
        if (!w.isRecover) {
            if (w.primaryNodeAddr.equals(w.realAddress))
                w.runForPrimaryDataNode();

            if (w.isPrimary) {
                {
                    //  make sure there is at least 2 standby node
                }
            } else {
                w.StandByDoRegister(w.primaryNodeAddr);
            }

            //保证Primary worker的StandBy node都注册好了之后再等待SetKeyRange
            if (w.isPrimary) {
                while (w.StandBySet.size() < 2)
                    TimeUnit.SECONDS.sleep(10);
            }

            if (w.isPrimary) {
                // if the worker is a new one, primary should call rpc SetKeyRange(startKey,endKey,true)
                // 对于leader Data Node来说，
                // 如果没有收到setKeyRange()就重新连接zookeeper
                // 这个情况适用于 InitialhashWorkers和ScaleOut 两者中的setKeyRange()

                TimeUnit.MINUTES.sleep(1);
                while (w.KeyStart == null) {
                    LOG.warn("the KeyRange is not initialized,retry");
                    w.zk.close();
                    try {
                        w.startZK();
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOG.error(String.valueOf(e));
                    }
                    w.runForPrimaryDataNode();
                    TimeUnit.MINUTES.sleep(2);
                }
            }
            LOG.info("register worker watcher");
            w.registerWorkerWatcher();
            //如果是primary data node 在初始化KeyRange之后注册watcher
            //如果是standby data node 直接注册watcher
        } else {
            // is recover
            w.DoRecover();
        }

        // 不论是否是primary data node都进行snapshot
        if (w.isRecover) {//means don't need recover or recover finish
            DoSnapshot();
        }
    }

    public static Integer Hash(String string) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(string);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }

    public void StandByDoRegister(String primaryNodeAddr) {
        int retrycounter = 0;
        while (retrycounter < 2) {
            try {
                String res = GetWorkerServiceByWorkerADDR(primaryNodeAddr).RegisterAsStandBy(this.realAddress);
                if (!res.equals("OK")) {
                    LOG.error("RegisterAsStandBy FAIL");
                } else {
                    LOG.info("RegisterAsStandBy OK");
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(String.valueOf(e));
            }
            retrycounter++;
        }
    }

    private void RecoverFromSnapshot() {
        LOG.info("[DoRecover]RecoverFromSnapshot");
        try {
            RingoDB.INSTANCE.recover();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
    }

    public void DoRecover() {
        LOG.info("[DoRecover] START");
        RecoverFromSnapshot();
        LOG.info("[DoRecover]register worker watcher");
        registerWorkerWatcher();
        LOG.info("[DoRecover]checkPrimaryDataNode");
        checkPrimaryDataNode();
    }

    public void registerWorkerWatcher() {
        while (true) {
            try {
                zk.exists("/workers/" + this.primaryNodeAddr, workerExistsWatcher);
                break;
            } catch (KeeperException.NoNodeException e) {
                LOG.info("NoNodeException in registerWorkerWatcher");
            } catch (KeeperException.ConnectionLossException ignored) {
                LOG.info("KeeperException.ConnectionLossException");
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                LOG.error(String.valueOf(e));
            }
        }
    }

    boolean checkPrimaryDataNode() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/workers/" + primaryNodeAddr, null, stat);
                isPrimary = new String(data).equals(realAddress);
                if (isPrimary) {
                    LOG.info(realAddress + "is already the primary Data Node");
                } else {
                    //register to the leader node
                    StandByDoRegister(new String(data));
                }
                return true;
            } catch (KeeperException.NoNodeException e) {
                LOG.info("checkPrimaryDataNode :NoNodeException");
                return false;
            } catch (KeeperException.ConnectionLossException ignored) {
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
                LOG.error(String.valueOf(e));
            }
        }
    }

    boolean checkNeedDataTransfer(String start, String end) {
        try {
            LOG.info("checkNeedDataTransfer: " + Hash(start) + " " + Hash(end));
            return RingoDB.INSTANCE.hasValueInRange(start, end);
        } catch (RingoDBException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        return false;
    }

    @Override
    public String DoTransfer(ConcurrentHashMap<String, String> data) {
        LOG.info("DoTransfer" + String.valueOf(data));

        try {
            RingoDB.INSTANCE.setMap(data);
            return "OK";
        } catch (RingoDBException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        return "ERR";
    }

    @Override
    public String RegisterAsStandBy(String StandByAddr) {
        // add to standbylist
        synchronized (StandBySet) {
            if (this.isPrimary) {
                LOG.info("StandBySet.add " + StandByAddr);
                StandBySet.add(StandByAddr);
            } else {
                LOG.info("not Primary Data NODE,can't add standby");
                return "ERR";
            }
        }
        return "OK";
    }

    // for send rpc to standby node to make data consistant
    WorkerService GetWorkerServiceByWorkerADDR(String WorkerAddr) {
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

    DataTransferService GetDataTransferServiceByWorkerADDR(String WorkerAddr) {
        ConsumerConfig<DataTransferService> consumerConfig = null;
        try {
            String workerip = WorkerAddr.split(":")[0];
            String workerport = WorkerAddr.split(":")[1];

            int port = Integer.parseInt(workerport);
            LOG.info("GetServiceByWorkerADDR " + workerip + ":" + port);
            consumerConfig = new ConsumerConfig<DataTransferService>()
                    .setInterfaceId(DataTransferService.class.getName()) // 指定接口
                    .setProtocol("bolt") // 指定协议
                    .setDirectUrl("bolt://" + workerip + ":" + port) // 指定直连地址
                    .setTimeout(2000)
                    .setRepeatedReferLimit(30); //允许同一interface，同一uniqueId，不同server情况refer 30次，用于单机调试

        } catch (SofaRpcException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }   // 生成代理类
        assert consumerConfig != null;
        return consumerConfig.refer();
    }

    public PrimaryService PrimaryConnection() {
        PrimaryService primaryService = null;
        try {
            Stat stat = new Stat();
            byte[] data = zk.getData("/primary", false, stat);
            String primaryip = new String(data);

            ConsumerConfig<PrimaryService> consumerConfig = new ConsumerConfig<PrimaryService>()
                    .setInterfaceId(PrimaryService.class.getName()) // 指定接口
                    .setProtocol("bolt") // 指定协议
                    .setDirectUrl("bolt://" + primaryip + ":12200") // 指定直连地址
                    .setTimeout(4000)//默认是3000
                    .setRepeatedReferLimit(500);//allow duplicate for tests
            // 生成代理类
            primaryService = consumerConfig.refer();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        assert primaryService != null;
        return primaryService;
    }

    public void notifyPrimaryDataTransferFinish() {
        try {
            PrimaryService primaryService = PrimaryConnection();
            String res = primaryService.notifyTransferFinish(this.primaryNodeAddr, this.KeyEnd);
            LOG.info("notifyTransferFinish " + res);
        } catch (Exception e) {
            LOG.error(String.valueOf(e));
            e.printStackTrace();
        }
    }

    @Override
    public String ResetKeyEnd(String oldKeyEnd, String NewKeyEnd, String WorkerReceiverADRR) {
        LOG.info("ready ResetKeyEnd to " + Hash(NewKeyEnd));
        if (checkNeedDataTransfer(NewKeyEnd, oldKeyEnd)) {
            try {
                ConcurrentHashMap<String, String> data = RingoDB.INSTANCE.SplitMap(NewKeyEnd, oldKeyEnd);
                this.KeyEnd = Hash(NewKeyEnd).toString();

                Thread t = new Thread(
                        () ->
                        {
                            LOG.info("do datatransfer: " + data);
                            String res = GetDataTransferServiceByWorkerADDR(WorkerReceiverADRR).DoTransfer(data);
                            //delete db data
                            if (res.equals("OK")) {
                                try {
                                    notifyPrimaryDataTransferFinish();
                                    // use primaryNodeAddr, because Hash(primaryNodeAddr) == KeyStart
                                    RingoDB.INSTANCE.TrunkMap(this.primaryNodeAddr, NewKeyEnd);
                                } catch (RingoDBException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                t.setName("datatransfer");
                t.start();
                return "WAIT";
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(String.valueOf(e));
            }
        } else {
            LOG.info("no need for datatransfer");
            this.KeyEnd = Hash(NewKeyEnd).toString();
            return "OK";
        }
        return "ERR";
    }

    @Override
    public String SetKeyRange(String keystart, String keyend, boolean dataTranfer) {
        if (this.primaryNodeAddr.equals(this.realAddress) && this.StandBySet.size() < 2) {
            LOG.warn("StandByNotReady");
            return "ERR";
        }
        if (this.KeyStart == null && this.KeyEnd == null) {
            if (!dataTranfer) {
                this.KeyStart = keystart;
                this.KeyEnd = keyend;
                LOG.info("initialize keyrage to " + keystart + ":" + keyend);
                if (this.isPrimary) {
                    Thread t = new Thread(() -> {
                        for (String standbyAddr : this.StandBySet) {
                            try {
                                String res = GetWorkerServiceByWorkerADDR(standbyAddr).SetKeyRange(this.KeyStart, this.KeyEnd, false);
                                if (!res.equals("OK")) {
                                    LOG.error(" Set STANDBY KeyRange FAIL");
                                } else {
                                    LOG.info("Set STANDBY KeyRange OK");
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                LOG.error(String.valueOf(e));
                            }
                        }
                    });
                    t.setName("Set STANDBY KeyRange");
                    t.start();
                }
                return "OK";
            }
        } else {
            assert this.KeyStart != null;
            if (this.KeyStart.equals(keystart) && this.KeyEnd.equals(keyend)) {
                LOG.info("reset keyrage to same value.");
                return "OK";
            }
        }
        if (dataTranfer) {
            //set KeyRange
            this.KeyStart = keystart;
            this.KeyEnd = keyend;
            LOG.info(realAddress + ":" + keystart + " " + keyend);
            // register as RPC Server
            registerDataTransferService();
            LOG.info(realAddress + " resgistered data transfer Service");
            // (RPC port for data transfer is the same as WorkerPort)
            return "OK";
        }
        return "ERR";
    }

    private void checkKeyRange(String keyString) throws MWException {
        int hashvalue = Hash(keyString);
        if (this.KeyStart != null) {
            if (hashvalue >= Integer.parseInt(this.KeyStart) && hashvalue < Integer.parseInt(this.KeyEnd)) {
                {

                }
            } else if ((hashvalue < Integer.parseInt(this.KeyEnd) | hashvalue >= Integer.parseInt(this.KeyStart)) && Integer.parseInt(this.KeyStart) > Integer.parseInt(this.KeyEnd)) {
                {
                }
            } else {
                throw new MWException("Dispatch wrong key to worker");
            }
        }
    }

    @Override
    public String PUT(String key, String value) {
        try {
            checkKeyRange(key);
            RingoDB.INSTANCE.Put(key, value);
            LOG.info("[DB EXECUTION]put" + key + ":" + value);
            {
                // 主节点通过异步的方式将新的数据同步到对应的从节点，
                // 不过在某些情况下会造成写丢失
                if (isPrimary) {
                    // concurrency control
                    // make writes to the same key sequential

                    CopyToStandBy copyToStandBy = new CopyToStandBy(key, value, StandBySet, EXECUTION.PUT, null);
                    copyToStandBy.setName("CopyToStandBy put " + key);
                    copyToStandBy.start();
//                    copyToStandBy.run();
//                    while (!lock.isWriteLocked()) {
//                        //保证copyToStandBy拿到锁
//                        // LOG.info("CopyToStandBy getting lock" + key);
//                    }
                    LOG.info("CopyToStandBy GET LOCK OF" + key);
                }
            }
            return "OK";
        } catch (RingoDBException | MWException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        return "ERR";
    }

    private ReentrantReadWriteLock GetRWlock(String key) {
        ReentrantReadWriteLock lock;
        if (keyRWLockMap.containsKey(key)) {
            lock = keyRWLockMap.get(key);
        } else {
            lock = new ReentrantReadWriteLock();
            keyRWLockMap.put(key, lock);
        }
        return lock;
    }

    @Override
    public String GET(String key) {
        try {
            assert isPrimary;
            checkKeyRange(key);
            String res = RingoDB.INSTANCE.Get(key);
            LOG.info("[DB EXECUTION] GET" + key + "value:" + res);
            return res;
        } catch (RingoDBException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
            if (e.getMessage().equals("key not exists")) {
                return "NO KEY";
            }
        } catch (MWException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        return "ERR";
    }

    @Override
    public String DELETE(String key) {
        try {
            checkKeyRange(key);
            RingoDB.INSTANCE.Delete(key);
            LOG.info("[DB EXECUTION] delete" + key);
            {
                // 主节点通过异步的方式将新的数据同步到对应的从节点，
                // 不过在某些情况下会造成写丢失
                if (isPrimary) {
                    // 只有拿到锁才能启动线程

                    CopyToStandBy copyToStandBy = new CopyToStandBy(key, null, StandBySet, EXECUTION.DELETE, null);
                    copyToStandBy.setName("CopyToStandBy delete " + key);
                    copyToStandBy.start();
//                    copyToStandBy.run();
//                    while (!lock.isWriteLocked()) {
//                        //保证copyToStandBy拿到锁
//                    }
                }
            }
            return "OK";
        } catch (RingoDBException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
            if (e.getMessage().equals("key not exists")) {
                return "NO KEY";
            }
        } catch (MWException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        return "ERR";
    }

    void registerDataTransferService() {
        try {

            LOG.info("registerDataTransferService PORT" + realAddress);
            ServerConfig serverConfig = (ServerConfig) new ServerConfig()
                    .setProtocol("bolt") // 设置一个协议，默认bolt
                    .setPort(Integer.parseInt(realPort))// 设置一个端口，即realPort
                    .setDaemon(true)// 守护线程
                    .setMaxThreads(20);

            ProviderConfig<DataTransferService> providerConfig = new ProviderConfig<DataTransferService>()
                    .setInterfaceId(DataTransferService.class.getName()) // 指定接口
                    .setRef(this)  // 指定实现
                    .setServer(serverConfig)// 指定服务端
                    .setRepeatedExportLimit(30); //允许同一interface，同一uniqueId，不同server情况发布30次，用于单机调试

            providerConfig.export(); // 发布服务
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
    }

    void registerRPCServices() {
        try {
            LOG.info("registerRPCServices PORT" + realAddress);
            ServerConfig serverConfig = new ServerConfig()
                    .setProtocol("bolt") // 设置一个协议，默认bolt
                    .setPort(Integer.parseInt(realPort)) // 设置一个端口，即realPort
                    .setDaemon(true); // 守护线程

            ProviderConfig<WorkerService> providerConfig = new ProviderConfig<WorkerService>()
                    .setInterfaceId(WorkerService.class.getName()) // 指定接口
                    .setRef(this)  // 指定实现
                    .setServer(serverConfig)// 指定服务端
                    .setRepeatedExportLimit(30); //允许同一interface，同一uniqueId，不同server情况发布30次，用于单机调试

            providerConfig.export(); // 发布服务
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
    }

    void startZK() {
        try {
            zk = new ZooKeeper(zookeeperaddress, 15000, this);

        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent e) {
        LOG.info(e.toString() + "," + zookeeperaddress);
    }

    void PrimaryDataNodeExists() {
        zk.exists("/workers" + primaryNodeAddr, null, workerExistsCallback, null);
    }

    void runForPrimaryDataNode() {
        if (!this.primaryNodeAddr.equals(this.realAddress) && this.KeyStart == null) {
            LOG.info("should let primaryNode get leadership in the initialization stage");
            return;
        }
        while (true) {
            try {
                LOG.info(realAddress + " is running for PrimaryDataNode");
                zk.create("/workers/" + primaryNodeAddr, realAddress.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                // create master znode should use CreateMode.EPHEMERAL
                // so the znode would be deleted when the connection is lost

                // reach here means create success, which means this is the leader node
                // this.StandBySet.clear();
                isPrimary = true;
                LOG.info(realAddress + " is PrimaryDataNode");
                break;
            } catch (KeeperException.NoNodeException e) {
                isPrimary = false;
                break;
            } catch (KeeperException.NodeExistsException e) {
                LOG.info("NodeExistsException,check if it's self");
                checkPrimaryDataNode();
                break;
            } catch (KeeperException.ConnectionLossException ignored) {
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            if (checkPrimaryDataNode()) break;
        }
    }

    public enum EXECUTION {
        PUT, DELETE
    }

    class CopyToStandBy extends Thread {
        public String key = null;
        public String value = null;
        public Set<String> standbySet = null;
        public EXECUTION execution = null;
        public ReentrantReadWriteLock lock = null;

        CopyToStandBy(String key, String value, Set<String> standbySet, EXECUTION execution, ReentrantReadWriteLock lock) {
            super();
            this.key = key;
            this.value = value;
            this.standbySet = standbySet;
            this.execution = execution;
            this.lock = lock;
        }

        public void run() {
            // LOG.info("copyToStandBy RUNNING");
            //先拿锁
            ReentrantReadWriteLock lock = GetRWlock(key);
            this.lock = lock;
            this.lock.writeLock().lock();
            for (String standbyAddr : this.standbySet) {
                LOG.info("ready to send " + standbyAddr);
                String res = null;
                int retrycounter = 0;
                assert execution != null;
                while (retrycounter < 2) {
                    try {
                        if (execution.equals(EXECUTION.PUT)) {
                            res = GetWorkerServiceByWorkerADDR(standbyAddr).PUT(key, value);
                            LOG.info(standbyAddr + " " + res);
                        }
                        if (execution.equals(EXECUTION.DELETE)) {
                            res = GetWorkerServiceByWorkerADDR(standbyAddr).DELETE(key);
                            LOG.info(standbyAddr + " " + res);
                        }
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
                    LOG.info(standbyAddr + " " + res);
                }
            }
            if (!lock.hasQueuedThreads()) {
                lock.writeLock().unlock();
                LOG.info(" keyRWLockMap.remove(key) ");
                keyRWLockMap.remove(key);
            } else {
                LOG.info("hasQueuedThreads");
                lock.writeLock().unlock();
            }
        }
    }
}
