import lib.PrimaryService;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClientTest {

    @Test
    public void isvalid() {
        assertTrue(Client.isvalid("PUT"));
        assertTrue(Client.isvalid("  put  "));
        assertFalse(Client.isvalid(" put put  "));

    }

    @Test
    public void PUTTEST() throws Exception {

        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        Thread.sleep(50);
        PrimaryService primaryService = client.PrimaryConnection();
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
    }

    @Test
    public void GETTEST() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
        assertEquals("apple", primaryService.GET("ringo"));
    }

    @Test
    public void nullvalueTest() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        assertEquals("OK", primaryService.PUT("null", null));
        assertEquals(null, primaryService.GET("null"));
    }

    @Test
    public void deleteTest() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
        assertEquals("OK", primaryService.PUT("null", null));
        assertEquals("OK", primaryService.DELETE("ringo"));
        assertEquals("OK", primaryService.DELETE("null"));
        assertEquals("NO KEY", primaryService.GET("ringo"));
        assertEquals("NO KEY", primaryService.GET("null"));
    }

    @Test
    public void DispathchToWorkersTest() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        PrimaryService primaryService = client.PrimaryConnection();
        assertEquals("OK", primaryService.PUT("apple", "ringo"));
        assertEquals("OK", primaryService.PUT("ringo", "apple"));
    }
}