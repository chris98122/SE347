import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import lib.KVService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
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
    String masterip;
    CountDownLatch latch;

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

    public KVService MasterConnection() {
        Stat stat = new Stat();
        byte data[];
        while (true) {
            try {
                data = zk.getData("/master", false, stat);
                break;
            } catch (KeeperException | InterruptedException e) {
                // just retry
            }
        }
        masterip = new String(data);
        System.out.println(new String(data));

        ConsumerConfig<KVService> consumerConfig = new ConsumerConfig<KVService>()
                .setInterfaceId(KVService.class.getName()) // 指定接口
                .setProtocol("bolt") // 指定协议
                .setDirectUrl("bolt://" + masterip + ":12200") // 指定直连地址
                .setTimeout(2000);
        // 生成代理类
        KVService kvService = consumerConfig.refer();
        return kvService;
    }

    void quit() throws InterruptedException {
        System.out.println("countOfRequest:"+countOfRequest.toString());
        if (countOfRequest.get()==0) {
            System.exit(0);
        }
    }

    void run() throws KeeperException, InterruptedException {
        KVService kvService = MasterConnection();
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
                                    System.out.println(kvService.PUT(finalKey, finalValue));
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
                                    System.out.println(kvService.GET(finalKey1));
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
                                    System.out.println(kvService.DELETE(finalKey2));
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
