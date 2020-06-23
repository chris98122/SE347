import lib.PrimaryService;

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
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip + ":1230" + workerID.toString(),
                                PrivateData.ip + ":1230" + workerID.toString(), "notrecover"
                        };
                        Worker w = new Worker(workerargs[0], workerargs[1], workerargs[2], workerargs[3]);
                        w.startZK();
                        w.registerRPCServices();// make sure the RPC can work, then register to zookeeper
                        w.runForPrimaryDataNode();// if the worker is a new one, master should call rpc SetKeyRange
                        Thread.sleep(12000);
                        //主动断开
                        System.out.println("worker" + workerID + "fail");
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
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip + ":1230" + workerID.toString(),
                                PrivateData.ip + ":1230" + workerID.toString(), "notrecover"
                        };
                        Worker.main(workerargs);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.setName("PrimaryWorker" + workerID);
        t.setDaemon(true);
        t.start();
    }

    static public void StartStandbyWorker(Integer workerID, Integer standByID) {
        Thread t = new Thread(
                () ->
                {
                    try {
                        String workerargs[] = {Config.zookeeperHost, PrivateData.ip + ":1230" + workerID.toString(),
                                PrivateData.ip + ":124" + workerID.toString() + standByID.toString(), "notrecover"
                        };
                        Worker.main(workerargs);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.setName("StandbyWorker" + workerID);
        t.setDaemon(true);
        t.start();
    }

    public static void StoreLargeData(Integer start, Integer datasize) throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        for (Integer i = start; i < start + datasize; i++) {
            System.out.println(primaryService.PUT(i.toString(), i.toString()));
        }
    }

    public static void GETLargeData(Integer start, Integer datasize) throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        for (Integer i = start; i < start + datasize; i++) {
            primaryService.GET(i.toString());
        }
    }

}
