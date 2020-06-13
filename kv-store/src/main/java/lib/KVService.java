package lib;

public interface KVService {
    // for communication between Master and Client
    // should be registeed in Master.java
    String PUT(String key, String value);

    String GET(String key);

    String DELETE(String key);
}
