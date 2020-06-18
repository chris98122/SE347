import com.alipay.sofa.rpc.config.ConsumerConfig;
import lib.DataTransferService;
import lib.PrimaryService;
import lib.WorkerService;
import org.junit.Test;

import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class ScaleOutTest {
    public static void main(String[] args) throws Exception, MWException {
        // TwoWorkerAddTest();
        LargeDataTransferTest(100);
    }

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

    static void StoreLargeData(int datasize) throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        for (Integer i = 0; i < datasize; i++) {
            primaryService.PUT(i.toString(), i.toString());
        }
    }

    @Test
    public static void LargeDataTransferTest(int datasize) throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入大量data
        StoreLargeData(datasize);

        //起1个普通worker
        Config.StartWorker(1);
        Thread.sleep(30000);
    }

    @Test
    public static void TwoWorkerAddTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入一些data
        StoreData();

        //起2个普通worker
        Config.StartWorker(1);
        Thread.sleep(2000);
        Config.StartWorker(2);
        Thread.sleep(30000);
    }

    @Test
    public void OneWorkerAddTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入一些data
        StoreData();

        Thread.sleep(2000);

        //起1个普通worker
        Config.StartWorker(1);
        Thread.sleep(30000);
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
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip, "12302"};
                        Worker w = new Worker(workerargs[0], workerargs[1], workerargs[2]);
                        w.registerRPCServices();
                        ConsumerConfig<WorkerService> consumerConfig = new ConsumerConfig<WorkerService>()
                                .setInterfaceId(WorkerService.class.getName()) // 指定接口
                                .setProtocol("bolt") // 指定协议
                                .setDirectUrl("bolt://" + PrivateData.ip + ":12301") // 指定直连地址
                                .setTimeout(2000)
                                .setRepeatedReferLimit(30); //允许同一interface，同一uniqueId，不同server情况refer 30次，用于单机调试
                        consumerConfig.refer().PUT("ringo", "apple");
                        consumerConfig.refer().DELETE("ringo");
                        DataTransferService S = w.GetServiceByWorkerADDR(PrivateData.ip + ":12301");
                        TreeMap<String, String> m = new TreeMap<>();
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