import lib.PrimaryService;
import org.junit.Test;

public class ScaleOutTest {
    public static void main(String[] args) throws Exception, MWException {
        // TwoWorkerAddTest();
        LargeDataTransferTest(100);
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
        while (true)
            Thread.sleep(3000);
    }

}