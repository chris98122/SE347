import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class MasterTest {

    public void showWorkerRange(ZooKeeper zk) throws KeeperException, InterruptedException {
        Integer keyStart = null;
        Integer keyEnd = null;
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            keyStart = Integer.valueOf(state.split("/")[0]);
            keyEnd = Integer.valueOf(state.split("/")[1]);
            Integer range = keyEnd > keyStart ? (keyEnd - keyStart) : Integer.MAX_VALUE - keyEnd + keyStart;
            double portion = range / (Integer.MAX_VALUE + 0.0) / 2;
            System.out.println("\t" + w + " keyrange" + String.valueOf(portion));
        }
    }

    @Test
    public void hashWorkers() throws Exception {
        for (Integer i = 0; i < 10; i++) {
            Integer finalI = i;
            new Thread(
                    () ->
                    {
                        try {
                            String worker2args[] = {Config.zookeeperHost, finalI.toString()};
                            Worker.main(worker2args);//worker 2
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
            ).start();
        }
        String args[] = {Config.zookeeperHost, "1"};
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            m.boostrap();
            Thread.sleep(600);
            m.InitialhashWorkers();
            showWorkerRange(m.zk);
        } else {
            System.out.println("Some one else is the leader.");
        }
    }
}