package lib;

public interface KVService {
    String PUT(String key, String value);

    String GET(String key);

    String DELETE(String key);
}
