package DB;

import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class RingoDBTest {

    @Test
    public void put() throws UnsupportedEncodingException, RingoDBException {
        RingoDB.INSTANCE.Put("ringo", "apple");
        System.out.println(RingoDB.INSTANCE.Get("ringo"));
    }

    @Test
    public void get() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void snapshot() throws IOException, RingoDBException, InterruptedException {
        RingoDB.INSTANCE.Put("ringo", "apple");
        RingoDB.INSTANCE.Put("papaya", "mugua");
        RingoDB.INSTANCE.snapshot();
        for (Map.Entry<String, String> item : RingoDB.INSTANCE.map.entrySet()) {
            System.out.println(item.getValue());
        }
        Thread.sleep(600);
    }

    @Test
    public void recover() throws RingoDBException, IOException, ClassNotFoundException {
        RingoDB.INSTANCE.recover();
        assertNotNull(RingoDB.INSTANCE.map);
        // 测试
        for (Map.Entry<String, String> item : RingoDB.INSTANCE.map.entrySet()) {
            System.out.println(item.getValue());
        }
    }

}