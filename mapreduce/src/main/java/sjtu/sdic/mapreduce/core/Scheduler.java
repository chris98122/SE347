package sjtu.sdic.mapreduce.core;

import sjtu.sdic.mapreduce.common.Channel;
import sjtu.sdic.mapreduce.common.DoTaskArgs;
import sjtu.sdic.mapreduce.common.JobPhase;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.rpc.Call;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Cachhe on 2019/4/22.
 */
public class Scheduler {

    /**
     * schedule() starts and waits for all tasks in the given phase (mapPhase
     * or reducePhase). the mapFiles argument holds the names of the files that
     * are the inputs to the map phase, one per map task. nReduce is the
     * number of reduce tasks. the registerChan argument yields a stream
     * of registered workers; each item is the worker's RPC address,
     * suitable for passing to {@link Call}. registerChan will yield all
     * existing registered workers (if any) and new ones as they register.
     *
     * @param jobName      job name
     * @param mapFiles     files' name (if in same dir, it's also the files' path)
     * @param nReduce      the number of reduce task that will be run ("R" in the paper)
     * @param phase        MAP or REDUCE
     * @param registerChan register info channel
     */


    public static void schedule(String jobName, String[] mapFiles, int nReduce, JobPhase phase, Channel<String> registerChan) {
        int nTasks = -1; // number of map or reduce tasks
        int nOther = -1; // number of inputs (for reduce) or outputs (for map)
        switch (phase) {
            case MAP_PHASE:
                nTasks = mapFiles.length;
                nOther = nReduce;
                break;
            case REDUCE_PHASE:
                nTasks = nReduce;
                nOther = mapFiles.length;
                break;
        }

        System.out.println(String.format("Schedule: %d %s tasks (%d I/Os)", nTasks, phase, nOther));

        /**
         // All ntasks tasks have to be scheduled on workers. Once all tasks
         // have completed successfully, schedule() should return.

         */
        //Hint: schedule() should send RPCs to the workers in parallel so that the workers can work on tasks concurrently. Learn how to use Java Thread API
        //  Hint: schedule() must wait for a worker to finish before it can give it another task. Try to exploit Channel.

        //To stop a thread, using interrupt().
        CountDownLatch latch = new CountDownLatch(nTasks);
        for (int i = 0; i < nTasks; i++) {
            DoTaskArgs arg = new DoTaskArgs(jobName, mapFiles[i], phase, i, nOther);
            Thread run = new Thread(() -> {
                String worker = "";
                try {
                    worker = registerChan.read();//return only if there's a value, otherwise blocking

                    Call.getWorkerRpcService(worker).doTask(arg);
                    if (latch != null) {
                        latch.countDown();
                    }
                    registerChan.write(worker);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

            });
            run.setDaemon(true);
            run.start();
        }
        try {
            latch.await();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println(String.format("Schedule: %s done", phase));
    }
}
