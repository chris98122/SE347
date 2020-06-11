import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientTest {

    @Test
    public void isvalid() {
        assertTrue(Client.isvalid("PUT"));
        assertTrue(Client.isvalid("  put  "));
        assertFalse(Client.isvalid(" put put  "));

    }
}