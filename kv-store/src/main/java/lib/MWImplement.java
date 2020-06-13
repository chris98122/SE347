package lib;

public class MWImplement implements MWService {
    @Override
    public String SetKeyRange(String keystart, String keyend) {
        return  keystart+" "+keyend;
    }

    @Override
    public String PUT(String key, String value) {
        System.out.println("put" + key + value);
        return "put" + key + value;
    }

    @Override
    public String GET(String key) {
        return "GET" + key;
    }

    @Override
    public String DELETE(String key) {
        return "delete" + key;
    }
}
