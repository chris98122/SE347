package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/21.
 */
public class WordCount {

    public static List<KeyValue> mapFunc(String file, String value) {
        // value contains the file content
        // Use Pattern and Matcher to extract words.
        //
        //  use words as the key
        List<KeyValue> res = new ArrayList<>();
        HashMap<String, Integer> wordcnt = new HashMap<>();
        Pattern p = Pattern.compile("[a-zA-Z0-9]+");
        Matcher m = p.matcher(value);
        while (m.find()) {
            // System.out.println(m.group());
            if (wordcnt.get(m.group()) == null)
                wordcnt.put(m.group(), 1);
            else
                wordcnt.put(m.group(), wordcnt.get(m.group()) + 1);
        }
        for (String key : wordcnt.keySet()) {
            KeyValue k = new KeyValue(key, String.valueOf(wordcnt.get(key)));
            res.add(k);
        }
        return res;
    }

    public static String reduceFunc(String key, String[] values) {
        // Your code here (Part II)
        // reduceFunc() will be called once for each key, with a slice of all the values generated by mapFunc() for that key.
        // It must return a string containing the total number of occurrences of the key.

        // Hint: Integer.valueOf(String str) could parse a valid string to integer.
        // System.out.println(key);
        Integer res = 0;
        for (int i = 0; i < values.length; i++) {
            res += Integer.valueOf(values[i]);
        }
        //System.out.println(String.valueOf(res));
        return String.valueOf(res);
    }

    public static void print(String[] splitted) {
        for (String val : splitted) {
            System.out.println(val);
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("error: see usage comments in file");
        } else if (args[0].equals("master")) {
            Master mr;

            String src = args[2];
            File file = new File(".");
            String[] files = file.list(new WildcardFileFilter(src));
            if (args[1].equals("sequential")) {
                mr = Master.sequential("wcseq", files, 3, WordCount::mapFunc, WordCount::reduceFunc);
            } else {
                mr = Master.distributed("wcdis", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], WordCount::mapFunc, WordCount::reduceFunc, 100, null);
        }
    }
}
