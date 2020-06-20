import DB.RingoDB;
import DB.RingoDBException;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import lib.DataTransferService;
import lib.WorkerService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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
    ZooKeeper zk;
    String KeyEnd = null;
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    LOG.info("retry register to zookeeper " + realAddress);
                    registerToZookeeper();//try agagin
                    break;
                case OK:
                    LOG.info("Registered successfully:" + realAddress);
                    break;
                case NODEEXISTS:
                    // RETRY JUST FOR EASY DEPLOYMENT, SHOULD MODIFY LATER
                    LOG.warn("Already registered:" + realAddress);
                    try {
                        Thread.sleep(600);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    registerToZookeeper();//try again
                    break;
                default:
                    LOG.error("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    volatile private String KeyStart = null;

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

    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(args[0], args[1], args[2], args[3], args[4]);
        w.startZK();
        w.registerRPCServices();// make sure the RPC can work, then register to zookeeper
        w.registerToZookeeper();
        // if the worker is a new one, master should call rpc SetKeyRange(startKey,endKey,true)

        // 等60秒，如果没有收到setKeyRange()就重新连接zookeeper
        // 这个情况适用于 InitialhashWorkers和ScaleOut 两者中的setKeyRange()
        TimeUnit.MINUTES.sleep(1);
        while (w.KeyStart == null) {
            LOG.warn("the scale out is not started,retry");
            w.zk.close();
            w.registerToZookeeper();
            TimeUnit.MINUTES.sleep(1);
        }

        int snapshotcounter = 0;
        while (true) {
            TimeUnit.HOURS.sleep(1);//一小时snapshot一次
            if (snapshotcounter < 10) {
                RingoDB.INSTANCE.snapshot();//十次snapshot
                snapshotcounter++;
            }
        }

    }

    public static Integer Hash(String string) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(string);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
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

    DataTransferService GetServiceByWorkerADDR(String WorkerAddr) {
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
                String res = GetServiceByWorkerADDR(WorkerReceiverADRR).DoTransfer(data);
                //delete db data
                if (res.equals("OK"))
                    RingoDB.INSTANCE.TrunkTreeMap(this.primaryNodeAddr, NewKeyEnd);
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
        if (this.KeyStart == null && this.KeyEnd == null) {
            if (!dataTranfer) {
                this.KeyStart = keystart;
                this.KeyEnd = keyend;
                LOG.info("initialize keyrage to " + keystart + ":" + keyend);
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

    @Override
    public String PUT(String key, String value) {
        try {
            checkKeyRange(key);
            RingoDB.INSTANCE.Put(key, value);
            LOG.info("[DB EXECUTION]put" + key + ":" + value);
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

    void registerToZookeeper() {
        zk.create("/workers/" + primaryNodeAddr, realAddress.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

}
