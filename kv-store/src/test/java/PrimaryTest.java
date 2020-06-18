import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class PrimaryTest {

    @Test
    public void hashWorkers() throws Exception, MWException {
        for (Integer i = 0; i < 10; i++) {
            Integer finalI = i;
            new Thread(
                    () ->
                    {
                        try {
                            String args[] = {Config.zookeeperHost, finalI.toString(), "1220" + finalI.toString()};
                            Worker.main(args);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
            ).start();
        }
        String args[] = {Config.zookeeperHost, "127.0.0.1"};
        Primary m = new Primary(args[0],args[1]);
        m.startZK();
        m.runForPrimary();
        if (m.isLeader) {
            m.boostrap();
            Thread.sleep(6000);
            m.InitialhashWorkers();
            Iterator iterator = m.workerkeymap.keySet().iterator();
            while (iterator.hasNext()) {
                String workerkey = (String) iterator.next();
                // System.out.println(key);
                List<String> list = (List<String>) m.workerkeymap.get(workerkey);
                System.out.println(list.get(0) + "-" + list.get(1));
                // else if((hashvalue >= keyEnd || hashvalue<keyStart)&& k
            }
        } else {
            System.out.println("Some one else is the leader.");
        }
    }

    @Test
    public void OneWorkerAddThenFailTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起1个会断开ZOOKEEPER的worker
        Config.StartWorkerCloseZookeeperAfterAwhile(1);

        Thread.sleep(30000);
    }

    @Test
    public void TwoWorkerAddThenFailTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起1个会断开ZOOKEEPER的worker
        Config.StartWorkerCloseZookeeperAfterAwhile(1);
        Config.StartWorkerCloseZookeeperAfterAwhile(2);
        Thread.sleep(30000);
    }

    @Test
    public void OneWorkerAddTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起1个普通worker
        Config.StartWorker(1);
        Thread.sleep(30000);
    }

    @Test
    public void TwoWorkerAddTest() throws Exception, MWException {
        Config.StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起2个普通worker
        Config.StartWorker(1);
        Config.StartWorker(2);
        Thread.sleep(30000);
    }

    @Test
    public void OneWorkerFailOneWorkerAddTest() throws Exception, MWException {
    }
}