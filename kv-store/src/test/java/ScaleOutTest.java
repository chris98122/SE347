import lib.PrimaryService;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScaleOutTest {
    void StoreData() throws Exception {
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
    public void TwoWorkerAddTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入一些data
        StoreData();

        //起2个普通worker
        Config.StartWorker(1);
        Config.StartWorker(2);
        Thread.sleep(30000);
    }
}