import lib.PrimaryService;
import org.junit.Test;

public class ScaleOutTest {
    public static void main(String[] args) throws Exception, MWException {
    }


    static void StoreLargeData(Integer start, Integer datasize) throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        for (Integer i = start; i < start + datasize; i++) {
            primaryService.PUT(i.toString(), i.toString());
        }
    }


    static void GETLargeData(Integer start, Integer datasize) throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        for (Integer i = start; i < start + datasize; i++) {
            primaryService.GET(i.toString());
        }
    }

    @Test
    public void ScaleOutWorkerTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入data
        StoreLargeData(0, 100);

        //因为DB是单例模式 所以不能起线程得起 WORKER 进程
        //在testScaleOut.sh起两个worker进程

        //持续在ScaleOut 过程中GET DATA
        GETLargeData(0, 100);
        Thread.sleep(12000);
        //primary线程退出

    }
}