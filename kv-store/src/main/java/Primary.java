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

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Primary implements Watcher, PrimaryService {
    private static final Logger LOG = LoggerFactory.getLogger(Primary.class);
    static String serverId;
    static Boolean isLeader;
    ZooKeeper zk;
    String hostPort;
    TreeMap<Integer, String> workermap = new TreeMap<>();
    // helper structure for calculating workerkeymap
    // stores startKey --> workerAddr mapping
    // e.g -399182218 -->  212.64.64.185:12201
    // NOT USED SINCE INITIAL HASH WORKERS, MAY CHANGE TO LOCAL VARIABLE

    TreeMap<String, List<String>> workerkeymap = new TreeMap<>();
    // stores workerAddr --> [KeyStart, KeyEnd] mapping
    // e.g 212.64.64.185:12201  --> [-399182218,1302869320]

    HashMap<String, ConsumerConfig<WorkerService>> workerConsumerConfigHashMap = new HashMap<String, ConsumerConfig<WorkerService>>();
    // stores workerAddr -->ConsumerConfig mapping

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
                        LOG.info("retry get workers");
                        getWorkers();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
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
                LOG.info("/workers changes");
                try {
                    getWorkers();//watcher是一次性的所以必须再次注册
                    //客户端 Watcher 回调的过程是一个串行同步的过程，所以另开线程处理


                    // 目前假设只有scale out 没有worker fail
                    Thread scaleOut = new Thread(() -> {
                        List<String> workerlist = null;//sync call

                        try {
                            workerlist = zk.getChildren("/workers", null);

                        } catch (KeeperException keeperException) {
                            keeperException.printStackTrace();
                        } catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
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


                                workerReiverService.SetKeyRange(Hash(workerReceiver).toString(), workerkeymap.get(workerSenderAddr).get(1), true);

                                // notify the workerSender's to reset keyrange
                                // workerSender do datatransfer
                                // reset workerkeymap
                                String newKeyEnd = Hash(workerReceiver).toString();
                                resetKeyRange(newKeyEnd, workerSenderAddr);
                                // if there are more new workers , have to wait til the former one is all set up.

                            }
                        }
                    });
                    scaleOut.start();

                } catch (KeeperException keeperException) {
                    keeperException.printStackTrace();
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
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

    public static void main(String args[])
            throws Exception, MWException {
        Primary m = new Primary(args[0]);
        m.startZK();
        m.runForPrimary();
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

    void resetKeyRange(String newKeyEnd, String workerAddr) {
        // block until the data transfer is done
        // reassign worker keyrange
    }

    public void getWorkers() throws KeeperException, InterruptedException {
        zk.getChildren("/workers", workersChangeWatcher, workerGetChildrenCallback, null);
        // register the workersChangeWatcher
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
                byte data[] = zk.getData("/primary", false, stat);
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
            } catch (KeeperException.ConnectionLossException e) {
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

}
