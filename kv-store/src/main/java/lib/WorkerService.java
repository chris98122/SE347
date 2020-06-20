package lib;

public interface WorkerService {
    // for communication between Primary and Worker
    // should be registered in Worker.java

    String SetKeyRange(String keystart, String keyend, boolean dataTranfer);

    String ResetKeyEnd(String oldKeyEnd, String NewKeyEnd, String WorkerReceiverADRR);

    String PUT(String key, String value);

    String GET(String key);

    String DELETE(String key);

    String RegisterAsStandBy(String StandByAddr);
    // Standby Data Node -> Primary Data Node
}
