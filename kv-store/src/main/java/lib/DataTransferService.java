package lib;

import java.util.concurrent.ConcurrentHashMap;

public interface DataTransferService {
    String DoTransfer(ConcurrentHashMap<String, String> data);

}
