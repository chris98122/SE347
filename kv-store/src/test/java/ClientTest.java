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
        MasterService masterService =  client.MasterConnection();
        assertTrue(masterService.PUT("ringo","apple").equals("OK"));

    }
}