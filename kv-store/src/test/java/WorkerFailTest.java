import org.junit.Test;

public class WorkerFailTest {
    @Test
    public void OneWorkerFail() throws Exception {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        //起1个会FAIL的worker
        Config.StartWorkerCloseZookeeperAfterAwhile(1);
        Thread.sleep(20);//保证Primary worker先抢占到领导权
        //起1个Standby worker
        Config.StartStandbyWorker(1);
        Thread.sleep(1200000);

    }
}
