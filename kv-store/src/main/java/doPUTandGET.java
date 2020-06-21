import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class doPUTandGET {
    public static void main(String[] args) throws Exception, MWException {
        test();
    }


    @Test
    public static void test() {
        try {
            Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok

            TimeUnit.SECONDS.sleep(15);

            // 存入data
            Config.StoreLargeData(0, 100);

            //因为DB是单例模式 所以不能起线程得起 WORKER 进程
            //在testScaleOut.sh起两个worker进程

            //持续在ScaleOut 过程中GET DATA
            Config.GETLargeData(0, 100);

            int storedatatcounter = 1;

            while (true) {
                if (storedatatcounter < 20) {
                    Config.StoreLargeData(storedatatcounter * 10, 10);
                    storedatatcounter++;
                } else {
                    TimeUnit.HOURS.sleep(1);
                }
                TimeUnit.SECONDS.sleep(30);
            }
        } catch (Exception e) {

        }
    }
}