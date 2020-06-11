package lib;

public class KVimplement implements KVService {
    @Override
    public String PUT(String key, String value) {
        key.hashCode();
        System.out.println("put" + key + value);
        return "put" + key + value;
    }

    @Override
    public String GET(String key) {
        System.out.println("GET" + key);
        return "GET" + key;
    }

    @Override
    public String DELETE(String key) {
        System.out.println("delete" + key);
        return "delete" + key;
    }
}
