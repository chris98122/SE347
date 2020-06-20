import lib.PrimaryService;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ScaleOutTest {
    public static void main(String[] args) throws Exception, MWException {
        ScaleOutWorkerTest();
    }


    @Test
    public static void ScaleOutWorkerTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);

        // 存入data
        Config.StoreLargeData(0, 100);

        //因为DB是单例模式 所以不能起线程得起 WORKER 进程
        //在testScaleOut.sh起两个worker进程

        //持续在ScaleOut 过程中GET DATA
        Config.GETLargeData(0, 100);

        int storedatatcounter = 1;

        while (true) {
            Thread.sleep(12000);
            if (storedatatcounter < 10) {
                Config.StoreLargeData(storedatatcounter * 100, 100);
                storedatatcounter++;
            } else {
                TimeUnit.HOURS.sleep(1);
            }
        }

    }
}