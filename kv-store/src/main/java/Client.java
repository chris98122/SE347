import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import lib.PrimaryService;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/*

3 operations for data accessing:
PUT(K,V)
READ(K)
DELETE(K)

 */
public class Client implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    protected AtomicInteger countOfRequest = new AtomicInteger(0);
    ZooKeeper zk;
    String hostPort;
    volatile String primaryip = null;
    AsyncCallback.DataCallback readprimaryCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData("/primary", false, readprimaryCallback, null);
                    LOG.info("retry get primary ip");
                    break;
                case OK:
                    primaryip = new String(bytes);
                    LOG.info("get primary ip " + primaryip);
                    break;
                case NONODE:
                    LOG.info("no node primary,retry");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    zk.getData("/primary", false, readprimaryCallback, null);
                    break;
                default:
                    LOG.error("something went wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    static boolean isvalid(String command) {
        switch (command.trim().toUpperCase()) {
            case "PUT":
            case "GET":
            case "DELETE":
            case "QUIT":
                return true;
        }
        return false;
    }

    public static void main(String args[]) throws Exception {
        Client client = new Client(args[0]);
        client.startZK();
        client.run();
    }

    void startZK() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public PrimaryService PrimaryConnection() {
        PrimaryService primaryService = null;
        try {
            zk.getData("/primary", false, readprimaryCallback, null);
            while (primaryip == null) {
                //block until get primary ip
            }
            ConsumerConfig<PrimaryService> consumerConfig = new ConsumerConfig<PrimaryService>()
                    .setInterfaceId(PrimaryService.class.getName()) // 指定接口
                    .setProtocol("bolt") // 指定协议
                    .setDirectUrl("bolt://" + primaryip + ":12200") // 指定直连地址
                    .setTimeout(2000)
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

    void quit() throws InterruptedException {
        System.out.println("countOfRequest:" + countOfRequest.toString());
        if (countOfRequest.get() == 0) {
            System.exit(0);
        }
    }

    void run() throws KeeperException, InterruptedException {
        PrimaryService primaryService = PrimaryConnection();
        Scanner scanner = new Scanner(System.in);
        String key = null, value = null;
        System.out.println("please enter PUT or GET or DELETE or QUIT");
        while (scanner.hasNext()) {
            //接受一行数据
            String command = scanner.nextLine();
            if (!isvalid(command)) {
                System.out.println("command " + command + " is invalid.");
                continue;
            }
            if (command.trim().toUpperCase().equals("QUIT")) {
                quit();
            }

            System.out.println("input key:");
            key = scanner.nextLine();
            switch (command.trim().toUpperCase()) {
                case "PUT":
                    System.out.println("input value:");
                    value = scanner.nextLine();
                    String finalKey = key;
                    String finalValue = value;
                    new Thread(
                            () ->
                            {
                                countOfRequest.incrementAndGet();
                                try {
                                    System.out.println(primaryService.PUT(finalKey, finalValue));
                                } catch (SofaRpcException e) {
                                    System.out.println(e);
                                } finally {
                                    countOfRequest.decrementAndGet(); // 计数-1
                                }
                            }
                    ).run();
                    break;
                case "GET":
                    String finalKey1 = key;
                    new Thread(
                            () ->
                            {
                                countOfRequest.incrementAndGet();
                                try {
                                    System.out.println(primaryService.GET(finalKey1));
                                } catch (SofaRpcException e) {
                                    System.out.println(e);
                                } finally {
                                    countOfRequest.decrementAndGet(); // 计数-1
                                }
                            }
                    ).run();
                    break;
                case "DELETE":
                    String finalKey2 = key;
                    new Thread(
                            () ->
                            {
                                countOfRequest.incrementAndGet();
                                try {
                                    System.out.println(primaryService.DELETE(finalKey2));
                                } catch (SofaRpcException e) {
                                    System.out.println(e);
                                } finally {
                                    countOfRequest.decrementAndGet(); // 计数-1
                                }
                            }
                    ).run();
                    break;
            }
            System.out.println("please enter PUT or GET or DELETE");
        }
    }

}
