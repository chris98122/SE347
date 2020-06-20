import org.junit.Test;

public class WorkerFailTest {
    @Test
    void OneWorkerFail() throws Exception {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入大量data
        Config.StoreLargeData(0, 100);

        //起1个普通worker
        Config.StartWorker(1);
        Thread.sleep(2000);//保证Primary worker先抢占到领导权
        //起1个Standby worker
        Config.StartStandbyWorker(1);
    }
}
