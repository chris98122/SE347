import com.alipay.sofa.rpc.config.ConsumerConfig;
import lib.DataTransferService;
import lib.PrimaryService;
import lib.WorkerService;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class ScaleOutTest {
    static void StoreData() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
        assertEquals("OK", primaryService.PUT("banana", "banana"));
        assertEquals("OK", primaryService.PUT("watermelon", "watermelon"));
        assertEquals("OK", primaryService.PUT("papaya", "papaya"));
        assertEquals("OK", primaryService.PUT("strawberry", "strawberry"));
        assertEquals("OK", primaryService.PUT("pear", "pear"));
        assertEquals("OK", primaryService.PUT("apricot", "apricot"));
        assertEquals("OK", primaryService.PUT("peach", "peach"));
        assertEquals("OK", primaryService.PUT("melon", "melon"));
        assertEquals("OK", primaryService.PUT("pineapple", "pineapple"));
    }


    @Test
    public void ScaleOutOneWorkerTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入大量data
        Config.StoreLargeData(0, 100);

        //起1个普通worker
        Config.StartWorker(1);

        Thread.sleep(3000);
        Config.StoreLargeData(101, 200);
        while (true)
            Thread.sleep(3000);
    }

    @Test
    public void ScaleOutWorkerTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入大量data
        Config.StoreLargeData(0, 100);

        //起1个普通worker
        Config.StartWorker(1);

        //因为DB是单例模式 所以不能起线程得起WORKER 进程
        //手动起两个worker进程

        //持续在ScaleOut 过程中GET DATA
        Config.GETLargeData(0, 100);
        Thread.sleep(12000);

    }


    @Test
    public void ScaleOutAndPUTTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入大量data
        Config.StoreLargeData(0, 100);

        //起1个普通worker
        Config.StartWorker(1);

        //持续在ScaleOut 过程中PUT DATA
        for (int i = 0; i < 10; i++) {
            Thread.sleep(10);
            Config.StoreLargeData(i * 100, 100);
        }

        Thread.sleep(12000);
    }

    @Test
    public void LargeDataTransferTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        String workerargs[] = {Config.zookeeperHost, PrivateData.ip + ":12302",
                PrivateData.ip + ":12302", "notrecover"
        };
        Worker w = new Worker(workerargs[0], workerargs[1], workerargs[2], workerargs[3]);
        w.registerRPCServices();

        ConsumerConfig<WorkerService> consumerConfig = new ConsumerConfig<WorkerService>()
                .setInterfaceId(WorkerService.class.getName()) // 指定接口
                .setProtocol("bolt") // 指定协议
                .setDirectUrl("bolt://" + PrivateData.ip + ":12301") // 指定直连地址
                .setTimeout(2000)
                .setRepeatedReferLimit(30); //允许同一interface，同一uniqueId，不同server情况refer 30次，用于单机调试

        consumerConfig.refer().PUT("ringo", "apple");
        consumerConfig.refer().DELETE("ringo");
        DataTransferService S = w.GetDataTransferServiceByWorkerADDR(PrivateData.ip + ":12301");
        ConcurrentHashMap<String, String> m = new ConcurrentHashMap<>();

        for (Integer i = 0; i < 100; i++) {
            m.put(i.toString(), i.toString());
        }

        S.DoTransfer(m);
        Thread.sleep(200);
    }

    @Test
    public void DataTransferRPCTest() throws Exception, MWException {
        //本地两个worker互联测试
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入一些data
        StoreData();

        //起1个普通worker
        Config.StartWorker(1);
        Thread.sleep(2000);

        Thread t = new Thread(
                () ->
                {
                    try {
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip + ":12302",
                                PrivateData.ip + ":12302", "NOTRECOVER"
                        };
                        Worker w = new Worker(workerargs[0], workerargs[1], workerargs[2], workerargs[3]);

                        w.registerRPCServices();
                        ConsumerConfig<WorkerService> consumerConfig = new ConsumerConfig<WorkerService>()
                                .setInterfaceId(WorkerService.class.getName()) // 指定接口
                                .setProtocol("bolt") // 指定协议
                                .setDirectUrl("bolt://" + PrivateData.ip + ":12301") // 指定直连地址
                                .setTimeout(2000)
                                .setRepeatedReferLimit(30); //允许同一interface，同一uniqueId，不同server情况refer 30次，用于单机调试
                        consumerConfig.refer().PUT("ringo", "apple");
                        consumerConfig.refer().DELETE("ringo");
                        DataTransferService S = w.GetDataTransferServiceByWorkerADDR(PrivateData.ip + ":12301");
                        ConcurrentHashMap<String, String> m = new ConcurrentHashMap<>();
                        m.put("ringo", "apple");
                        S.DoTransfer(m);
                        Thread.sleep(2000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.setName("GetServiceByWorkerADDR test");
        t.start();
        Thread.sleep(30000);
    }
}