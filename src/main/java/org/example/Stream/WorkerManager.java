package org.example.Stream;

import org.example.FileSystem.HashFunction;
import org.example.FileSystem.Sender;
import org.example.entities.*;

import java.io.File;
import java.util.*;
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

    public static void initializeFailedWorker(Worker worker, int failedWorkerId) {
        //TODO fetch the logs and fill in the processed list tuples to filter duplicates
        // and also get the state if op2
        HashSet<String> processedTuples = getLogs(failedWorkerId, worker.type);
        worker.setTuplesReceived(processedTuples);
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

    public static void restartSource(int workerId, int lineNo){
        Worker worker = workers.get(workerId);
        System.out.println("Source " + workerId + " restarting source from " + lineNo);
        worker.stopWorker();
        String[] range = worker.ranges.split(",");
        worker.ranges = lineNo + "," + range[1];
        worker.restartWorker();
        worker.start();
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

    public static HashSet<String> getLogs(int failedWorkerId, String type){
        Sender sender = new Sender();
        String fileName = failedWorkerId + "_" + type +"_log.txt";
        int fileNameHash = HashFunction.hash(fileName);
        Member member = MembershipList.getMemberById(fileNameHash);
        String IpAddress = member.getIpAddress();
        String port = member.getPort();
        int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
        try {
            String response = "Unsuccessful";
            while(response.equals("Unsuccessful")) {
                Map<String, Object> messageContent = new HashMap<>();
                messageContent.put("messageName", "get_log");
                messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
                messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
                messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
                messageContent.put("fileReceiverPort", String.valueOf(fileReceiverPort));
                messageContent.put("msgId", FDProperties.generateRandomMessageId());
                messageContent.put("localFileName", fileName);
                messageContent.put("hyDFSFileName", fileName);
                String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
                Message msg = new Message("get_log",
                        String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                        senderPort,
                        messageContent);
                response = sender.sendMessage(IpAddress, Integer.parseInt(port), msg);
                if(response.equals("Successful")) {
                    break;
                }else {
                    Thread.sleep(1000);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        //Read the logs
        try (Scanner scanner = new Scanner(new File("local\\" + fileName))) {
            String line;
            HashSet<String> processedTuples = new HashSet<>();
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();
                if(line.contains("received")) {
                    String[] temp = line.split(":");
                    String tempID = temp[1].replace(" ", "");
                    processedTuples.add(tempID);
                }
            }
            return processedTuples;
        }catch (Exception e){
            e.printStackTrace();
        }
        if(type.equals("op2")){
            getState(failedWorkerId,type);
        }
        return null;
    }

    public static void getState(int failedWorkerId, String type){
        Sender sender = new Sender();
        String fileName = failedWorkerId + "_" + type +"_data.ser";
        int fileNameHash = HashFunction.hash(fileName);
        Member member = MembershipList.getMemberById(fileNameHash);
        String IpAddress = member.getIpAddress();
        String port = member.getPort();
        int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
        try {
            String response = "Unsuccessful";
            while(response.equals("Unsuccessful")) {
                Map<String, Object> messageContent = new HashMap<>();
                messageContent.put("messageName", "get_log");
                messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
                messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
                messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
                messageContent.put("fileReceiverPort", String.valueOf(fileReceiverPort));
                messageContent.put("msgId", FDProperties.generateRandomMessageId());
                messageContent.put("localFileName", fileName);
                messageContent.put("hyDFSFileName", fileName);
                String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
                Message msg = new Message("get_log",
                        String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                        senderPort,
                        messageContent);
                response = sender.sendMessage(IpAddress, Integer.parseInt(port), msg);
                if(response.equals("Successful")) {
                    break;
                }else {
                    Thread.sleep(1000);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
