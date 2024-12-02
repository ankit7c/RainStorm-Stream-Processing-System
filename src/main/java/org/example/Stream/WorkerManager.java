package org.example.Stream;

import org.example.entities.FDProperties;
import org.example.entities.MembershipList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

//TODO update this class to manage multiple worker nodes
public class WorkerManager {
    public static ConcurrentSkipListMap<Integer,Worker> workers = new ConcurrentSkipListMap<>();
    static int id = 0;
    static int currReceiverPort = Integer.parseInt(String.valueOf(FDProperties.getFDProperties().get("machinePort"))) + 20;
    //Function to initialize Worker
    public static int initializeWorker(Worker worker) {
        int workerId = Integer.parseInt(String.valueOf(MembershipList.selfId) + id++);
        workers.put(workerId,worker);
        worker.receiverPort = currReceiverPort;
        currReceiverPort = currReceiverPort + 10;
        return workerId;
    }

    public static void startWorker(int id) {
        Worker worker = workers.get(id);
        try{
            Thread.sleep(2000);
            worker.start();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
