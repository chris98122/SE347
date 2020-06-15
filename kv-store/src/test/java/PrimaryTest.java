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
                            String worker2args[] = {Config.zookeeperHost, finalI.toString(), "1220" + finalI.toString()};
                            Worker.main(worker2args);//worker 2
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
            Thread.sleep(600);
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
}