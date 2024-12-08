package org.example.Stream;

import org.example.entities.ExecutorProperties;
import org.example.entities.FDProperties;
import org.example.entities.Member;
import org.example.entities.MembershipList;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

//TODO update this class to manage multiple worker nodes
public class WorkerManager {
    public static Member leader;
    public static ConcurrentSkipListMap<Integer,Worker> workers = new ConcurrentSkipListMap<>();
    static int id = 0;
    static int currReceiverPort = Integer.parseInt(String.valueOf(FDProperties.getFDProperties().get("machinePort"))) + 20;

    public static final Map<Integer, BlockingQueue<QueueData>> consumerQueues = new ConcurrentHashMap<>();

    //Function to initialize Worker
    public static int initializeWorker(Worker worker) {
        ExecutorProperties.initialize();
        int workerId = Integer.parseInt(String.valueOf(MembershipList.selfId) + id++);
        workers.put(workerId,worker);
        worker.selfId = workerId;
        worker.receiverPort = currReceiverPort;
        consumerQueues.put(worker.selfId,new LinkedBlockingQueue<>());
        System.out.println("Worker " + workerId + " initialized with current port " + currReceiverPort);
        currReceiverPort = currReceiverPort + 10;
        return workerId;
    }

    public static void startWorker(int workerid) {
        Worker worker = workers.get(workerid);
        try{
            worker.start();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void killWorker(int workerid) {
        Worker worker = workers.get(workerid);
        System.out.println("Worker " + workerid + " killed");
        worker.stopWorker();
        System.out.println("Removing worker " + workerid);
        workers.remove(workerid);
        consumerQueues.remove(workerid);

    }

    public static void assignTuple(int workerId, int senderId, Member member, Tuple tuple, String tupleType) {
        try {
            System.out.println("Assigning tuple : " + workerId);
            consumerQueues.get(workerId).put(new QueueData(senderId, workerId, member, tuple, tupleType, tuple.getId()));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void assignAck(int workerId, int senderId, Member member, String tupleId){
        try {
            consumerQueues.get(workerId).put(new QueueData(senderId, workerId, member, "ack", tupleId));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
