import lib.PrimaryService;
import org.junit.Test;

public class ConcurrencyTest {
    @Test
    public void concurrencyPUT() throws InterruptedException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        //构造一堆在同一个KEY上put的client线程
        for (int i = 0; i < 30; i++) {
            ClientPUT c = new ClientPUT();
            c.setDaemon(true);
            c.setName("client" + i);
            c.start();
        }
        Thread.sleep(100000);
    }

    class ClientPUT extends Thread {
        public Integer start = 0;
        public Integer datasize = 1;

        public void run() {
            Client client = new Client(Config.zookeeperHost);
            try {
                client.startZK();
            } catch (Exception e) {
                e.printStackTrace();
            }
            PrimaryService primaryService = client.PrimaryConnection();
            for (int j = 0; j < 10; j++) {
                for (Integer i = start; i < start + datasize; i++) {
                    System.out.println(primaryService.PUT(i.toString(), i.toString()));
                }
            }
        }
    }
}
