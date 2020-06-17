package DB;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.TreeMap;


public enum RingoDB implements DB {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(RingoDB.class);
    static Integer snapshot_version = 0;
    TreeMap<String, String> map = new TreeMap<String, String>();
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
    }

    public synchronized boolean hasValueInRange(String keyStart, String KeyEnd) throws RingoDBException {
        checkKey(keyStart);
        checkKey(KeyEnd);
        for (String key : map.keySet()) {
            if (inRange(key, keyStart, KeyEnd)) {
                return true;
            }
        }

        return false;
    }

    private boolean inRange(String key, String keyStart, String KeyEnd) {
        int keystart = Integer.parseInt(keyStart);
        int keyend = Integer.parseInt(KeyEnd);
        int newkey = Hash(key);
        if (keystart < keyend) {
            return newkey >= keystart && newkey < keyend;
        }
        if (keystart > keyend) {
            return newkey >= keystart || newkey < keyend;
        }
        return false;
    }

    public TreeMap<String, String> SplitTreeMap(String keyStart, String KeyEnd) throws RingoDBException {
        TreeMap<String, String> res = new TreeMap<>();

        for (String key : map.keySet()) {
            if (inRange(key, keyStart, KeyEnd)) {
                res.put(key, map.get(key));
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
    //to use RingoDB just call RingoDB.INSTANCE. flush()
}