package DB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;


enum RingoDB implements DB {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(RingoDB.class);
    static Integer snapshot_version = 0;
    HashMap<String, String> map = new HashMap<String, String>();
    String SNAPSHOT_DIR = "./";

    @Override
    public void Put(String key, String value) throws RingoDBException, UnsupportedEncodingException {
        checkKey(key);
        map.put(key, value);
    }

    @Override
    public String Get(String key) throws RingoDBException {
        checkKey(key);
        return map.get(key);
    }

    @Override
    public void Delete(String key) throws RingoDBException {
        checkKey(key);
        map.remove(key);
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

        HashMap<String, String> m1 = (HashMap<String, String>) ois.readObject();
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