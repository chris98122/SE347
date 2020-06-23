package lib;

public interface PrimaryService {
    // for communication between Master and Client
    // should be registeed in Primary.java
    String PUT(String key, String value);

    String GET(String key);

    String DELETE(String key);

    String notifyTransferFinish(String WorkerSenderAddr, String newKeyEnd);
}
