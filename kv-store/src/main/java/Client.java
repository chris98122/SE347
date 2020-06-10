import com.alipay.sofa.rpc.config.ConsumerConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Scanner;

/*

3 operations for data accessing:
PUT(K,V)
READ(K)
DELETE(K)

 */
public class Client implements Watcher {
    ZooKeeper zk;
    String hostPort;
    String masterip;

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    static boolean isvalid(String command) {
        return true;
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


    void run() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte data[];
        while (true) {
            try {
                data = zk.getData("/master", false, stat);
                break;
            } catch (KeeperException.NoNodeException e) {
                // just retry
            }
        }
        masterip = new String(data);
        System.out.println(new String(data));

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            //接受一行数据
            String command = scanner.nextLine();
            if (isvalid(command)) {

                ConsumerConfig<KVService> consumerConfig = new ConsumerConfig<KVService>()
                        .setInterfaceId(KVService.class.getName()) // 指定接口
                        .setProtocol("bolt") // 指定协议
                        .setDirectUrl("bolt://" + masterip + ":12200"); // 指定直连地址
                // 生成代理类
                KVService kvService = consumerConfig.refer();
                while (true) {
                    System.out.println(kvService.doKV(command));
                    try {
                        Thread.sleep(2000);
                    } catch (Exception e) {
                    }
                }
            } else {
                System.out.println("command " + command + " is invalid");
            }

        }
    }

}
