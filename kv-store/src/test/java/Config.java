public class Config {
    public static final String zookeeperHost = "112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183";

    static public void StartPrimary() {
        //起primary
        String args[] = {Config.zookeeperHost, PrivateData.ip};
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
        runm.setDaemon(true);
        runm.start();
    }

    static public void StartWorkerCloseZookeeperAfterAwhile(Integer workerID) {
        Thread t = new Thread(
                () ->
                {
                    try {
                        //primary data node
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip, "1230" + workerID.toString(),
                                PrivateData.ip, "1230" + workerID.toString()
                        };
                        Worker w = new Worker(workerargs[0], workerargs[1], workerargs[2], workerargs[3], workerargs[4]);
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
        t.setDaemon(true);
        t.start();
    }

    static public void StartWorker(Integer workerID) {
        Thread t = new Thread(
                () ->
                {
                    try {
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip, "1230" + workerID.toString(),
                                PrivateData.ip, "1230" + workerID.toString()
                        };
                        Worker.main(workerargs);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.setName("worker" + workerID);
        t.setDaemon(true);
        t.start();
    }
}
