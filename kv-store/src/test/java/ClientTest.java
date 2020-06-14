import lib.MasterService;
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
        MasterService masterService = client.MasterConnection();
        assertEquals("OK", masterService.PUT("ringo", "apple"));
        assertEquals("OK", masterService.PUT("ringo", "apple"));
        assertEquals("OK", masterService.PUT("ringo", "apple"));
    }

    @Test
    public void GETTEST() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        MasterService masterService = client.MasterConnection();
        assertEquals("OK", masterService.PUT("ringo", "apple"));
        assertEquals("apple", masterService.GET("ringo"));
    }

    @Test
    public void nullvalueTest() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        MasterService masterService = client.MasterConnection();
        assertEquals("OK", masterService.PUT("null", null));
        assertEquals(null, masterService.GET("null"));
    }
    @Test
    public void deleteTest() throws Exception {
        Client client = new Client(Config.zookeeperHost);
        client.startZK();
        MasterService masterService = client.MasterConnection();
        assertEquals("OK", masterService.DELETE("ringo"));
        assertEquals("OK", masterService.DELETE("null" ));
        assertEquals("NO KEY", masterService.GET("ringo" ));
    }
}