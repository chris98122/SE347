import DB.RingoDB;
import DB.RingoDBException;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import lib.DataTransferService;
import lib.WorkerService;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.TreeMap;

public class Worker implements Watcher, WorkerService, DataTransferService {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final String WorkerPort;
    ZooKeeper zk;
    String hostPort;
    String serverId;
    String KeyStart = null;
    String KeyEnd = null;
    int DataTransferoffset = 10;
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    LOG.info("retry register to zookeeper " + serverId);
                    registerToZookeeper();//try agagin
                    break;
                case OK:
                    LOG.info("Registered successfully:" + serverId);
                    break;
                case NODEEXISTS:
                    // RETRY JUST FOR EASY DEPLOYMENT, SHOULD MODIFY LATER
                    LOG.warn("Already registered:" + serverId);
                    try {
                        Thread.sleep(600);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    registerToZookeeper();//try agagin
                    break;
                default:
                    LOG.error("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    Worker(String hostPort, String WorkerIP, String WorkerPort) throws UnknownHostException {
        this.WorkerPort = WorkerPort;
        this.hostPort = hostPort;
        serverId = WorkerIP + ':' + WorkerPort;
        // WorkerID is used to distinguish different worker processes on one machine
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(args[0], args[1], args[2]);
        w.startZK();
        w.registerRPCServices();// make sure the RPC can work, then register to zookeeper
        w.registerToZookeeper();
        // if the worker is a new one, master should call rpc SetKeyRange(startKey,endKey,true)

        while (true) {
            Thread.sleep(600);
        }

    }

    boolean checkNeedDataTransfer(String start, String end) {
        try {
            LOG.info("checkNeedDataTransfer: " + start + " " + end);
            return RingoDB.INSTANCE.hasValueInRange(start, end);
        } catch (RingoDBException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String DoTransfer(TreeMap<String, String> data) {
        try {
            RingoDB.INSTANCE.setMap(data);
            return "OK";
        } catch (RingoDBException e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    DataTransferService GetServiceByWorkerADDR(String WorkerAddr) {
        ConsumerConfig<DataTransferService> consumerConfig = null;
        try {
            String workerip = WorkerAddr.split(":")[0];
            String workerport = WorkerAddr.split(":")[1];

            int port = Integer.parseInt(workerport) + DataTransferoffset;
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
    public String ResetKeyEnd(String NewKeyEnd, String WorkerReceiverADRR) {
        LOG.info("ready ResetKeyEnd to " + NewKeyEnd);
        if (checkNeedDataTransfer(NewKeyEnd, this.KeyEnd)) {
            try {
                TreeMap<String, String> data = RingoDB.INSTANCE.SplitTreeMap(this.KeyEnd, NewKeyEnd);
                LOG.info("do datatransfer: " + data);
                String res = GetServiceByWorkerADDR(WorkerReceiverADRR).DoTransfer(data);
                //delete db data
                return res;
            } catch (Exception e) {
                LOG.error(String.valueOf(e));
            }
        } else {
            LOG.info("no need for datatransfer");
            this.KeyEnd = NewKeyEnd;
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
            // register as RPC Server
            registerDataTransferService();
            LOG.info(serverId + " resgistered data transfer Service");
            // (RPC port for data transfer is the same as WorkerPort)
            return "OK";
        }
        return "ERR";
    }

    @Override
    public String PUT(String key, String value) {
        try {
            RingoDB.INSTANCE.Put(key, value);
            LOG.info("[DB EXECUTION]put" + key + ":" + value);
            return "OK";
        } catch (RingoDBException e) {
            e.printStackTrace();
        }
        return "ERR";
    }

    @Override
    public String GET(String key) {
        try {
            String res = RingoDB.INSTANCE.Get(key);
            LOG.info("[DB EXECUTION] GET" + key + "value:" + res);
            return res;
        } catch (RingoDBException e) {
            e.printStackTrace();
            if (e.getMessage().equals("key not exists")) {
                return "NO KEY";
            }
        }
        return "ERR";
    }

    @Override
    public String DELETE(String key) {
        try {
            RingoDB.INSTANCE.Delete(key);
            LOG.info("[DB EXECUTION] delete" + key);
            return "OK";
        } catch (RingoDBException e) {
            e.printStackTrace();
            if (e.getMessage().equals("key not exists")) {
                return "NO KEY";
            }
        }
        return "ERR";
    }

    void registerDataTransferService() {
        try {

            int port = Integer.parseInt(WorkerPort) + DataTransferoffset;
            LOG.info("registerDataTransferService PORT" + port);
            ServerConfig serverConfig = (ServerConfig) new ServerConfig()
                    .setProtocol("bolt") // 设置一个协议，默认bolt
                    .setPort(port) // 设置一个端口，即args[2]
                    .setDaemon(true);// 守护线程

            ProviderConfig<DataTransferService> providerConfig = new ProviderConfig<DataTransferService>()
                    .setInterfaceId(DataTransferService.class.getName()) // 指定接口
                    .setRef(this)  // 指定实现
                    .setServer(serverConfig)// 指定服务端
                    .setRepeatedExportLimit(30); //允许同一interface，同一uniqueId，不同server情况发布30次，用于单机调试

            providerConfig.export(); // 发布服务
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void registerRPCServices() {
        try {
            ServerConfig serverConfig = new ServerConfig()
                    .setProtocol("bolt") // 设置一个协议，默认bolt
                    .setPort(Integer.parseInt(WorkerPort)) // 设置一个端口，即args[2]
                    .setDaemon(true); // 守护线程

            ProviderConfig<WorkerService> providerConfig = new ProviderConfig<WorkerService>()
                    .setInterfaceId(WorkerService.class.getName()) // 指定接口
                    .setRef(this)  // 指定实现
                    .setServer(serverConfig)// 指定服务端
                    .setRepeatedExportLimit(30); //允许同一interface，同一uniqueId，不同server情况发布30次，用于单机调试

            providerConfig.export(); // 发布服务
        } catch (Exception e) {
            e.printStackTrace();
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

    void registerToZookeeper() {
        zk.create("/workers/" + serverId, "UnHashed".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

}
