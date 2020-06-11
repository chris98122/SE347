package lib;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class Util {
    public static void printthreads() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
// 遍历线程组树，获取根线程组
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }
// 激活的线程数加倍
        int estimatedSize = topGroup.activeCount() * 2;
        Thread[] slackList = new Thread[estimatedSize];
// 获取根线程组的所有线程
        int actualSize = topGroup.enumerate(slackList);
// copy into a list that is the exact size
        Thread[] list = new Thread[actualSize];
        System.arraycopy(slackList, 0, list, 0, actualSize);
        System.out.println("Thread list size == " + list.length);
        for (Thread thread : list) {
            System.out.println(thread.getName());
        }
    }

    public static void getWorkers(ZooKeeper zk) throws KeeperException, InterruptedException {
        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ":" + state);
        }
        ;
    }

    public static void getTasks(ZooKeeper zk) throws KeeperException, InterruptedException {
        System.out.println("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
    }
}
