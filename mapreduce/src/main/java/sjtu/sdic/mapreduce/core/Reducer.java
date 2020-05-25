package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Reducer {

    /**
     * doReduce manages one reduce task: it should read the intermediate
     * files for the task, sort the intermediate key/value pairs by key,
     * call the user-defined reduce function {@code reduceF} for each key,
     * and write reduceF's output to disk.
     * <p>
     * You'll need to read one intermediate file from each map task;
     * {@code reduceName(jobName, m, reduceTask)} yields the file
     * name from map task m.
     * <p>
     * Your {@code doMap()} encoded the key/value pairs in the intermediate
     * files, so you will need to decode them. If you used JSON, you can refer
     * to related docs to know how to decode.
     * <p>
     * In the original paper, sorting is optional but helpful. Here you are
     * also required to do sorting. Lib is allowed.
     * <p>
     * {@code reduceF()} is the application's reduce function. You should
     * call it once per distinct key, with a slice of all the values
     * for that key. {@code reduceF()} returns the reduced value for that
     * key.
     * <p>
     * You should write the reduce output as JSON encoded KeyValue
     * objects to the file named outFile. We require you to use JSON
     * because that is what the merger than combines the output
     * from all the reduce tasks expects. There is nothing special about
     * JSON -- it is just the marshalling format we chose to use.
     * <p>
     * Your code here (Part I).
     *
     * @param jobName    the name of the whole MapReduce job
     * @param reduceTask which reduce task this is
     * @param outFile    write the output here
     * @param nMap       the number of map tasks that were run ("M" in the paper)
     * @param reduceF    user-defined reduce function
     */
    public static void doReduce(String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceF) {
        // read the intermediate  files for the task,
        String inFile = Utils.reduceName(jobName, 0, reduceTask);


        String json_kvpairs = readUTF(inFile);
        // sort the intermediate key/value pairs by key,

        List<KeyValue> kvpairs_list = JSONArray.parseArray(json_kvpairs, KeyValue.class);


        //  call the user-defined reduce function {@code reduceF} for each key,
        HashMap<String,String> content = new HashMap<>();
        for (KeyValue pair : kvpairs_list) {
            String[] strArray = {pair.value};
            //  System.out.println(jsonObject.toString());
            content.put(pair.key,reduceF.reduce(pair.key,strArray));

        }
        writeFile(outFile,JSONObject.toJSONString(content));

        // write the reduce output as JSON encoded KeyValue objects to the file named outFile

    }

    public static void printListKV(List<KeyValue> kvpairs_list) {
        for (KeyValue pair : kvpairs_list) {
            System.out.println(pair.key + ":" + pair.value);
        }
    }

    public static void writeFile(String inFile, String content) {
        try {
            BufferedWriter  out = new BufferedWriter(new FileWriter(inFile,true));
            out.newLine();
            out.write( content );
            out.close();
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static String readUTF(String inFile) {
        try {
            File fileDir = new File(inFile);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(fileDir), "UTF8"));

            String str;
            String res = "";

            while ((str = in.readLine()) != null) {
                res = res + str + "\n";
            }
            in.close();
            return res;
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return "";
    }

}
