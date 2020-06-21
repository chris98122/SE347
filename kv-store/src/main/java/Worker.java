import DB.RingoDB;
import DB.RingoDBException;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import lib.DataTransferService;
import lib.WorkerService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Worker implements Watcher, WorkerService, DataTransferService {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // primary data node metadata
    private final String primaryNodeIP;
    private final String primaryNodePort;
    private final String primaryNodeAddr;
    // real node metadata
    private final String realAddress;
    private final String realIP;
    private final String realPort;
    private final String zookeeperaddress;
    ZooKeeper zk;
    volatile private Set<String> StandBySet = new HashSet<String>();
    volatile private String KeyStart = null;
    private String KeyEnd = null;
    // stores workerAddr -->ConsumerConfig mapping
    volatile private HashMap<String, ConsumerConfig<WorkerService>> workerConsumerConfigHashMap = new HashMap<String, ConsumerConfig<WorkerService>>();
    volatile private boolean isPrimary;
    Watcher workerExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            LOG.info("workerExistsWatcher" + e.getPath());
            PrimaryDataNodeExists();//watcher是一次性的所以必须再次注册
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

    Worker(String zookeeperaddress, String primaryNodeIP, String primaryNodePort, String realIP, String realPort) throws UnknownHostException {

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
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(args[0], args[1], args[2], args[3], args[4]);
        w.startZK();
        w.registerRPCServices();// make sure the RPC can work, then register to zookeeper

        //保证Primary worker先抢占到领导权
        if (w.primaryNodeAddr.equals(w.realAddress))
            w.runForPrimaryDataNode();

        if (w.isPrimary) {
            {
                //  make sure there is at least 2 standby node
            }
        } else {
            try {
                String res = w.GetWorkerServiceByWorkerADDR(w.primaryNodeAddr).RegisterAsStandBy(w.realAddress);
                if (!res.equals("OK")) {
                    LOG.error(" RegisterAsStandBy FAIL");
                } else {
                    LOG.info("RegisterAsStandBy OK");
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(String.valueOf(e));
            }
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
        w.PrimaryDataNodeExists();
        //如果是primary data node 在初始化KeyRange之后注册watcher
        //如果是standby data node 直接注册watcher

        // 不论是否是primary data node都进行snapshot
        int snapshotcounter = 0;

        while (true) {
            TimeUnit.HOURS.sleep(1);//一小时snapshot一次
            RingoDB.INSTANCE.snapshot();//保存最新的2次snapshot
            snapshotcounter++;
            if (snapshotcounter >= 3) {
                // 删除snapshot
                RingoDB.INSTANCE.delete_oldest_snapshot();
            }
        }

    }

    public static Integer Hash(String string) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(string);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }


    boolean checkPrimaryDataNode() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/workers/" + primaryNodeAddr, workerExistsWatcher, stat);
                isPrimary = new String(data).equals(realAddress);
                if (isPrimary) {
                    LOG.info(realAddress + "is already the primary Data Node");
                } else {
                    //register to the leader node
                    try {
                        String res = GetWorkerServiceByWorkerADDR(new String(data)).RegisterAsStandBy(this.realAddress);
                        if (!res.equals("OK")) {
                            LOG.error(" RegisterAsStandBy FAIL");
                        } else {
                            LOG.info("RegisterAsStandBy OK");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOG.error(String.valueOf(e));
                    }
                }
                return true;
            } catch (KeeperException.NoNodeException e) {
                LOG.info("checkPrimaryDataNode :NoNodeException");
                return false;
            } catch (KeeperException.ConnectionLossException ignored) {
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
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
    public String DoTransfer(TreeMap<String, String> data) {
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

        } catch (Exception e) {
            LOG.error(Arrays.toString(e.getStackTrace()));
        }   // 生成代理类
        assert consumerConfig != null;
        return consumerConfig.refer();
    }

    @Override
    public String ResetKeyEnd(String oldKeyEnd, String NewKeyEnd, String WorkerReceiverADRR) {
        LOG.info("ready ResetKeyEnd to " + Hash(NewKeyEnd));
        if (checkNeedDataTransfer(NewKeyEnd, oldKeyEnd)) {
            try {
                TreeMap<String, String> data = RingoDB.INSTANCE.SplitTreeMap(NewKeyEnd, oldKeyEnd);

                LOG.info("do datatransfer: " + data);
                String res = GetDataTransferServiceByWorkerADDR(WorkerReceiverADRR).DoTransfer(data);
                //delete db data
                if (res.equals("OK")) {
                    RingoDB.INSTANCE.TrunkTreeMap(this.primaryNodeAddr, NewKeyEnd);
                    this.KeyEnd = Hash(NewKeyEnd).toString();
                }
                // use primaryNodeAddr, because Hash(primaryNodeAddr) == KeyStart
                return res;
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
                    CopyToStandBy copyToStandBy = new CopyToStandBy(key, value, StandBySet, EXECUTION.PUT);
                    copyToStandBy.setName("CopyToStandBy put" + key);
                    copyToStandBy.start();
                }
            }
            return "OK";
        } catch (RingoDBException | MWException e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
        return "ERR";
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
                    CopyToStandBy copyToStandBy = new CopyToStandBy(key, null, StandBySet, EXECUTION.DELETE);
                    copyToStandBy.setName("CopyToStandBy delete" + key);
                    copyToStandBy.start();
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
                    .setDaemon(true);// 守护线程

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
        zk.exists("/workers" + primaryNodeAddr, workerExistsWatcher, workerExistsCallback, null);
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

        CopyToStandBy(String key, String value, Set<String> standbySet, EXECUTION execution) {
            super();
            this.key = key;
            this.value = value;
            this.standbySet = standbySet;
            this.execution = execution;

        }

        public void run() {
            LOG.info("copyToStandBy RUNNING");
            for (String standbyAddr : this.standbySet) {
                LOG.info("ready to send " + standbyAddr);
                try {
                    assert execution != null;
                    if (execution.equals(EXECUTION.PUT)) {
                        String res = GetWorkerServiceByWorkerADDR(standbyAddr).PUT(key, value);
                        LOG.info(standbyAddr + " " + res);
                    }
                    if (execution.equals(EXECUTION.DELETE)) {
                        String res = GetWorkerServiceByWorkerADDR(standbyAddr).DELETE(key);
                        LOG.info(standbyAddr + " " + res);
                    }
                } catch (Exception e) {

                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    String sStackTrace = sw.toString(); // stack trace as a string
                    LOG.error(sStackTrace);
                    LOG.error(String.valueOf(e));
                }
            }
        }
    }

}
