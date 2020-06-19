package DB;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;


public enum RingoDB implements DB {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(RingoDB.class);
    static Integer snapshot_version = 0;
    TreeMap<String, String> map = new TreeMap<String, String>(new KeyComparator());
    String SNAPSHOT_DIR = "./";

    public static Integer Hash(String string) {
        //加密后的字符串
        String encodeStr = DigestUtils.md5Hex(string);
        //System.out.println("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr.hashCode();
    }

    @Override
    public synchronized void Put(String key, String value) throws RingoDBException {
        checkKey(key);
        map.put(key, value);
    }

    @Override
    public synchronized String Get(String key) throws RingoDBException {
        checkKey(key);
        checkKeyExists(key);
        return map.get(key);
    }

    @Override
    public synchronized void Delete(String key) throws RingoDBException {
        checkKey(key);
        checkKeyExists(key);
        map.remove(key);
    }

    public void setMap(TreeMap<String, String> data) throws RingoDBException {
        assert map.isEmpty();
        map = data;
        printDBContent();
    }

    public synchronized boolean hasValueInRange(String keyStart, String KeyEnd) throws RingoDBException {
        checkKey(keyStart);
        checkKey(KeyEnd);
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        if (keystart < keyend) {
            LOG.info("submap" + map.subMap(keyStart, KeyEnd));
            return map.subMap(keyStart, KeyEnd).size() >= 1;
        }
        if (keystart > keyend) {
            for (String key : map.keySet()) {
                int newkey = Hash(key);
                return newkey >= keystart || newkey < keyend;
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

    public void TrunkTreeMap(String keyStart, String KeyEnd) throws RingoDBException {
        //save keyStart->KeyEnd
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        printDBContent();
        LOG.info("TrunkTreeMap" + keystart + " " + keyend);
        if (keystart < keyend) {
            TreeMap<String, String> newmap = new TreeMap<String, String>(new KeyComparator());
            for (String key : map.keySet()) {
                if (Hash(key) >= keystart && Hash(key) < keyend) {
                    newmap.put(key, map.get(key));
                }
            }
            map = newmap;
        }
        if (keystart > keyend) {
            TreeMap newmap = new TreeMap<String, String>(new KeyComparator());
            for (String key : map.keySet()) {
                if (inRange(key, keyStart, KeyEnd)) {
                    newmap.put(key, map.get(key));
                }
            }
            map = newmap;
        }
        printDBContent();
    }

    public TreeMap<String, String> SplitTreeMap(String keyStart, String KeyEnd) throws RingoDBException {
        TreeMap<String, String> res = null;
        int keystart = Hash(keyStart);
        int keyend = Hash(KeyEnd);
        if (keystart < keyend) {
            SortedMap<String, String> s = map.subMap(keyStart, KeyEnd);
            res = new TreeMap<>(s);
            return res;
        }
        if (keystart > keyend) {
            res = new TreeMap<String, String>(new KeyComparator());
            for (String key : map.keySet()) {
                if (inRange(key, keyStart, KeyEnd)) {
                    res.put(key, map.get(key));
                }
            }
        }
        return res;
    }

    void checkKeyExists(String key) throws RingoDBException {
        if (!map.containsKey(key)) {
            //key 不存在
            throw new RingoDBException("key not exists");
        }
    }

    String generate_snapshot_name() {
        snapshot_version++;
        return "snapshot-" + snapshot_version.toString();
    }

    String get_snapshot_name() {
        File dir = new File(SNAPSHOT_DIR); //要遍历的目录
        //System.out.println(dir);
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                //System.out.println(children[i]);
                if (children[i].split("-").length > 1 && children[i].split("-")[0].equals("snapshot")) {
                    try {
                        Integer version = Integer.valueOf(children[i].split("-")[1]);
                        snapshot_version = Math.max(snapshot_version, version);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return "snapshot-" + snapshot_version.toString();
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
                        oout.writeObject(map);
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

    public void recover() throws IOException, ClassNotFoundException {
        // create an ObjectInputStream for the file we created before
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(get_snapshot_name()));

        TreeMap<String, String> m1 = (TreeMap<String, String>) ois.readObject();
        if (map.isEmpty()) {
            map = m1;
        } else {
            // recovery
        }
    }

    private void checkKey(String key) throws RingoDBException {
        if (key == null) {
            throw new RingoDBException("key is empty");
        }
    }

    class KeyComparator implements Comparator<String>//比较器
    {
        @Override
        public int compare(String o1, String o2) {
            assert (o1 != null && o2 != null);
            return new Integer(Hash(o1)).compareTo(Hash(o2));
        }
    }
    //to use RingoDB just call RingoDB.INSTANCE. flush()
}