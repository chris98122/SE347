import lib.PrimaryService;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class doPUTandGET {
    public static void main(String[] args) throws Exception, MWException {
        Thread t = new Thread(
                () ->
                {
                    try {
                        NormalPUTGETTest();
                        DOSomePUTEverySecond();
                        TimeUnit.SECONDS.sleep(100);
                        concurrencyTest();
                        TimeUnit.SECONDS.sleep(30);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.start();
    }


    @Test
    public static void NormalPUTGETTest() {
        try {
            Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok

            TimeUnit.SECONDS.sleep(5);

            // 存入data
            Config.StoreLargeData(0, 100);

            //因为DB是单例模式 所以不能起线程得起 WORKER 进程
            //在testScaleOut.sh起两个worker进程

            //持续在ScaleOut 过程中GET DATA
            Config.GETLargeData(0, 100);

        } catch (Exception e) {
        }

    }

    public static void DOSomePUTEverySecond() throws InterruptedException {
        Thread t = new Thread(
                () ->
                {
                    Client client = new Client(Config.zookeeperHost);
                    try {
                        client.startZK();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    PrimaryService primaryService = client.PrimaryConnection();
                    Random ran1 = new Random(10);
                    while (true) {
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        Integer input = ran1.nextInt(Integer.MAX_VALUE);
                        System.out.println(primaryService.PUT(input.toString(), input.toString()));
                    }
                });
        t.setName("DOSomePUTeverysecond");
        t.start();
    }

    public static void concurrencyTest() throws InterruptedException {
        //构造一堆在同一个KEY上put的client线程
        for (int i = 0; i < 5; i++) {
            ClientPUT c = new ClientPUT();
            c.setDaemon(true);
            c.setName("client" + i);
            c.start();
        }
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