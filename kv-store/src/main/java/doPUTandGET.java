import lib.PrimaryService;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class doPUTandGET {
    public static void main(String[] args) throws Exception, MWException {
        NormalPUTGETTest();
        concurrencyTest();//worker fail during the period
        TimeUnit.SECONDS.sleep(30);
        concurrencyTest();
        TimeUnit.SECONDS.sleep(30);
    }


    @Test
    public static void NormalPUTGETTest() {
        try {
            Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok

            TimeUnit.SECONDS.sleep(15);

            // 存入data
            Config.StoreLargeData(0, 100);

            //因为DB是单例模式 所以不能起线程得起 WORKER 进程
            //在testScaleOut.sh起两个worker进程

            //持续在ScaleOut 过程中GET DATA
            Config.GETLargeData(0, 100);

            int storedatatcounter = 1;

            while (true) {
                if (storedatatcounter < 20) {
                    Config.StoreLargeData(storedatatcounter * 10, 10);
                    storedatatcounter++;
                } else {
                    break;
                }
                TimeUnit.SECONDS.sleep(30);
            }
        } catch (Exception e) {
        }

    }

    public static void concurrencyTest() throws InterruptedException {
        //构造一堆在同一个KEY上put的client线程
        for (int i = 0; i < 30; i++) {
            ClientPUT c = new ClientPUT();
            c.setDaemon(true);
            c.setName("client" + i);
            c.start();
        }
        TimeUnit.SECONDS.sleep(30);
    }

    static class ClientPUT extends Thread {
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