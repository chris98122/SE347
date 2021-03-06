package DB;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public enum RingoDB implements DB {
    INSTANCE;
    private static final Logger LOG = LoggerFactory.getLogger(RingoDB.class);
    static AtomicInteger snapshot_version = new AtomicInteger(0);
    private static String SNAPSHOT_DIR = null;
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();

    public static Integer Hash(String string) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(string);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }

    @Override
    public void Put(String key, String value) throws RingoDBException {
        checkKey(key);
        map.put(key, value);
    }

    @Override
    public String Get(String key) throws RingoDBException {
        checkKey(key);
        checkKeyExists(key);
        return map.get(key);
    }

    @Override
    public void Delete(String key) throws RingoDBException {
        checkKey(key);
        checkKeyExists(key);
        map.remove(key);
    }

    public void setMap(ConcurrentHashMap<String, String> data) throws RingoDBException {
        try {
            assert map.isEmpty();
            map = data;
            printDBContent();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.valueOf(e));
        }
    }

    public boolean hasValueInRange(String keyStart, String KeyEnd) throws RingoDBException {
        // hasValueInRange用于检查是否有key在一致性哈希环上指定的范围内
        checkKey(keyStart);
        checkKey(KeyEnd);
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        if (keystart < keyend) {
            for (String key : map.keySet()) {
                int newkey = Hash(key);
                if (newkey >= keystart && newkey < keyend)
                    return true;
            }
        }
        if (keystart > keyend) {
            for (String key : map.keySet()) {
                int newkey = Hash(key);
                if (newkey >= keystart || newkey < keyend)
                    return true;
            }
        }
        //map.subMap(K startKey，K endKey)方法用于返回由参数中指定范围的键定义的映射的部分或部分

        return false;
    }

    private void printDBContent() {
        LOG.info(String.valueOf(map));
    }

    private boolean inRange(String key, String keyStart, String KeyEnd) {
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        int newkey = Hash(key);
        assert (keystart > keyend);
        return newkey >= keystart || newkey < keyend;
    }

    public void TrunkMap(String keyStart, String KeyEnd) throws RingoDBException {
        // save keyStart->KeyEnd
        // TrunkMap用于仅仅保留在一致性哈希环上指定的范围内的键值对，而删除其他键值对
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        printDBContent();
        LOG.info("TrunkMap" + keystart + " " + keyend);
        if (keystart < keyend) {
            ConcurrentHashMap<String, String> newmap = new ConcurrentHashMap<String, String>();
            for (String key : map.keySet()) {
                if (Hash(key) >= keystart && Hash(key) < keyend) {
                    newmap.put(key, map.get(key));
                }
            }
            map = newmap;
        }
        if (keystart > keyend) {
            ConcurrentHashMap<String, String> newmap = new ConcurrentHashMap<String, String>();
            for (String key : map.keySet()) {
                if (inRange(key, keyStart, KeyEnd)) {
                    newmap.put(key, map.get(key));
                }
            }
            map = newmap;
        }
        printDBContent();
    }

    public ConcurrentHashMap<String, String> SplitMap(String keyStart, String KeyEnd) throws RingoDBException {
        ConcurrentHashMap<String, String> res = null;
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        if (keystart < keyend) {
            LOG.info("SplitMap");
            res = new ConcurrentHashMap<String, String>();
            for (String key : map.keySet()) {
                if (Hash(key) >= keystart && Hash(key) < keyend) {
                    res.put(key, map.get(key));
                }
            }
        }
        if (keystart > keyend) {
            res = new ConcurrentHashMap<String, String>();
            for (String key : map.keySet()) {
                if (inRange(key, keyStart, KeyEnd)) {
                    res.put(key, map.get(key));
                }
            }
        }
        return res;
    }

    private void checkKeyExists(String key) throws RingoDBException {
        if (!map.containsKey(key)) {
            //key 不存在
            throw new RingoDBException.KeyNotExists();
        }
    }

    private String generate_snapshot_name() {
        return "snapshot-" + snapshot_version.incrementAndGet();
    }

    public boolean delete_oldest_snapshot() {
        File file = new File(get_oldest_snapshot_name());
        if (!file.exists()) {
            LOG.error("delete_oldest_snapshot FAIL!" + get_oldest_snapshot_name() + "not exist");
            return false;
        } else {
            return file.delete();
        }
    }

    private String SNAPSHOT_DIR() {
        if (SNAPSHOT_DIR == null) {
            File directory = new File("");
            SNAPSHOT_DIR = directory.getAbsolutePath(); //获取绝对路径。
        }
        return SNAPSHOT_DIR;
    }

    private String get_oldest_snapshot_name() {
        File dir = new File(SNAPSHOT_DIR()); //要遍历的目录
        Integer oldest_snapshot_version = Integer.MAX_VALUE;
        //System.out.println(dir);
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                //System.out.println(children[i]);
                if (children[i].split("-").length > 1 && children[i].split("-")[0].equals("snapshot")) {
                    try {
                        Integer version = Integer.valueOf(children[i].split("-")[1]);
                        oldest_snapshot_version = Math.min(oldest_snapshot_version, version);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return "snapshot-" + oldest_snapshot_version.toString();
    }

    private String get_newest_snapshot_name() throws RingoDBException {
        LOG.info(" get_newest_snapshot_name()");

        File dir = new File(SNAPSHOT_DIR()); //要遍历的目录
        LOG.info(dir.getPath());
        Integer res = 0;

        if (dir.isDirectory()) {
            String[] children = dir.list();
            int snapshotnum = 0;
            for (int i = 0; i < children.length; i++) {
                //System.out.println(children[i]);
                if (children[i].split("-").length > 1 && children[i].split("-")[0].equals("snapshot")) {
                    snapshotnum++;
                }
            }
            if (snapshotnum == 0) {
                throw new RingoDBException.NoSnapshot();
            }
            for (int i = 0; i < children.length; i++) {
                //System.out.println(children[i]);
                if (children[i].split("-").length > 1 && children[i].split("-")[0].equals("snapshot")) {
                    try {
                        Integer version = Integer.valueOf(children[i].split("-")[1]);
                        LOG.info("[RingoDB]" + version.toString());
                        res = Math.max(snapshot_version.get(), version);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            throw new RingoDBException("wrong directory");
        }
        LOG.info("snapshot-" + res.toString());
        return "snapshot-" + res.toString();
    }

    public void snapshot() throws IOException {
        new Thread(
                () ->
                {
                    // create a new file with an ObjectOutputStream
                    FileOutputStream out = null;
                    try {
                        out = new FileOutputStream(generate_snapshot_name());
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    ObjectOutputStream oout = null;
                    try {
                        oout = new ObjectOutputStream(out);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    // write something in the file
                    try {
                        assert oout != null;
                        ConcurrentHashMap<String, String> copy = new ConcurrentHashMap<String, String>();
                        copy.putAll(map);
                        oout.writeObject(copy);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        oout.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        ).start();
    }

    public void recover() throws IOException, ClassNotFoundException, RingoDBException {
        // create an ObjectInputStream for the file we created before
        String filename = null;
        try {
            filename = get_newest_snapshot_name();
        } catch (RingoDBException.NoSnapshot e) {
            LOG.warn("no snapshot for recovery");
            return;
        }

        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename));
        ConcurrentHashMap<String, String> m1 = (ConcurrentHashMap<String, String>) ois.readObject();
        if (map.isEmpty()) {
            map = m1;
        } else {
            LOG.error("[recover] map is not empty");
            throw new RingoDBException.MapNotEmpty();
        }
        printDBContent();// check if recover ok
    }

    private void checkKey(String key) throws RingoDBException {
        if (key == null) {
            throw new RingoDBException.KeyEmpty();
        }
    }

    //to use RingoDB just call RingoDB.INSTANCE. flush()
}