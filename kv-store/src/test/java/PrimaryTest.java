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
        String args[] = {Config.zookeeperHost, "1"};
        Primary m = new Primary(args[0]);
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

    void StartPrimary() {
        //起primary
        String args[] = {Config.zookeeperHost, "1"};
        Thread runm = new Thread(
                () ->
                {
                    try {
                        Primary.main(args);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        runm.setName("primary");
        runm.start();
    }

    void StartWorkerCloseZookeeperAfterAwhile(Integer workerID) {
        Thread t = new Thread(
                () ->
                {
                    try {
                        String workerargs[] = {Config.zookeeperHost, "127.0.0.1", "1230" + workerID.toString()};
                        Worker w = new Worker(workerargs[0], workerargs[1], workerargs[2]);
                        w.startZK();
                        w.registerRPCServices();// make sure the RPC can work, then register to zookeeper
                        w.registerToZookeeper();// if the worker is a new one, master should call rpc SetKeyRange
                        Thread.sleep(2000);
                        //主动断开
                        w.zk.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.setName("worker" + workerID);
        t.start();
    }

    void StartWorker(Integer workerID) {
        Thread t = new Thread(
                () ->
                {
                    try {
                        String workerargs[] = {Config.zookeeperHost, "127.0.0.1", "1230" + workerID.toString()};
                        Worker.main(workerargs);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.setName("worker" + workerID);
        t.start();
    }

    @Test
    public void OneWorkerAddThenFailTest() throws Exception, MWException {
        StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起1个会断开ZOOKEEPER的worker
        StartWorkerCloseZookeeperAfterAwhile(1);

        Thread.sleep(30000);
    }

    @Test
    public void TwoWorkerAddThenFailTest() throws Exception, MWException {
        StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起1个会断开ZOOKEEPER的worker
        StartWorkerCloseZookeeperAfterAwhile(1);
        StartWorkerCloseZookeeperAfterAwhile(2);
        Thread.sleep(30000);
    }

    @Test
    public void OneWorkerAddTest() throws Exception, MWException {
        StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起1个普通worker
        StartWorker(1);
        Thread.sleep(30000);
    }

    @Test
    public void TwoWorkerAddTest() throws Exception, MWException {
        StartPrimary();//原本有两个worker已经在运行，所以initializeworker ok
        Thread.sleep(12000);
        //起2个普通worker
        StartWorker(1);
        StartWorker(2);
        Thread.sleep(30000);
    }

    @Test
    public void OneWorkerFailOneWorkerAddTest() throws Exception, MWException {
    }
}