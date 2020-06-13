package lib;

public interface WorkerService {
    // for communication between Master and Worker
    //should be registeed in Worker.java
    String SetKeyRange(String keystart, String keyend);

    String PUT(String key, String value);

    String GET(String key);

    String DELETE(String key);
}
