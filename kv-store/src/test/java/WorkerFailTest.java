import org.junit.Test;

public class WorkerFailTest {
    @Test
    public void StandByRegisterTest() throws InterruptedException {

        // 起2个普通的worker
        Config.StartWorker(1);
        Config.StartWorker(2);


        // 对每个WORKER起2个Standby worker
        Config.StartStandbyWorker(1, 1);
        Config.StartStandbyWorker(1, 2);
        Config.StartStandbyWorker(2, 1);
        Config.StartStandbyWorker(2, 2);

        Thread.sleep(1000);

        Config.StartPrimary();
        Thread.sleep(120000);
    }

    @Test
    public void OneWorkerFail() throws Exception {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        //起1个会FAIL的worker
        Config.StartWorkerCloseZookeeperAfterAwhile(1);
        //起2个Standby worker
        Config.StartStandbyWorker(1, 1);
        Config.StartStandbyWorker(1, 2);
        Thread.sleep(1200000);

    }

    @Test
    public void TwoWorkerFail() throws Exception {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        //起2个会FAIL的worker
        Config.StartWorkerCloseZookeeperAfterAwhile(1);
        Config.StartWorkerCloseZookeeperAfterAwhile(2);


        // 对每个WORKER起2个Standby worker
        Config.StartStandbyWorker(1, 1);
        Config.StartStandbyWorker(1, 2);
        Config.StartStandbyWorker(2, 1);
        Config.StartStandbyWorker(2, 2);

        Thread.sleep(1200000);

    }
}
