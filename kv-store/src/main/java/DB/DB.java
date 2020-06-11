package DB;

public interface DB {
    void Put(String key, String value) throws Exception;

    String Get(String key) throws Exception;

    void Delete(String key) throws Exception;

}
