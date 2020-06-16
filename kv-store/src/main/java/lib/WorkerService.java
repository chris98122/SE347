package lib;

public interface WorkerService {
    // for communication between Primary and Worker
    //should be registeed in Worker.java
    String SetKeyRange(String keystart, String keyend, boolean dataTranfer);

    String ResetKeyEnd(String keyend, String WorkerReceiverADRR);

    String PUT(String key, String value);

    String GET(String key);

    String DELETE(String key);
}
