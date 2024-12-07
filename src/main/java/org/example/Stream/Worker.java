package org.example.Stream;

import org.example.Executor.OperationExecutor;
import org.example.FileSystem.FileSender;
import org.example.FileSystem.HashFunction;
import org.example.FileSystem.Sender;
import org.example.entities.Member;
import org.example.entities.MembershipList;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

//TODO based on role a Worker thread will be created and a specific function will be called
public class Worker extends Thread {

    public int selfId;
    List<Member> source;
    List<Member> op1;
    List<Member> op2;

    Map<Integer,Member> sources = new ConcurrentSkipListMap<>();
    Map<Integer,Member> op1s = new ConcurrentSkipListMap<>();
    Map<Integer,Member> op2s = new ConcurrentSkipListMap<>();
    String ranges;
    String type;
    String filename;
    String destFileName;
    String operationName;
    //Worker ID, Receiver Port
    HashMap<Integer, Integer> receiverPorts = new HashMap<>();
    HashMap<Integer, Integer> partMapOp1 = new HashMap<>();
    HashMap<Integer, Integer> partMapOp2 = new HashMap<>();
    private Sender sender = new Sender();

    public List<QueueData> tuplesSent = new CopyOnWriteArrayList<>();
    public List<QueueData> tuplesToBeSent= new CopyOnWriteArrayList<>();
    public List<QueueData> tuplesReceived = new CopyOnWriteArrayList<>();
    public List<String> logList = new CopyOnWriteArrayList<>();

    public int receiverPort;

    public Worker(String type, List<Member> source , List<Member> op1, List<Member> op2, String ranges, String filename, String destFileName, String operationName) {
        this.source = source;
        this.op1 = op1;
        this.op2 = op2;
        this.ranges = ranges;
        this.type = type;
        this.filename = filename;
        this.destFileName = destFileName;
        this.operationName = operationName;
    }

    public void setReceiverPorts(ArrayList<String> receiverPorts) {
        for (String port : receiverPorts) {
            if(!port.isBlank()) {
                String[] s = port.split(":");
                System.out.println("I am saving details for : " + port);
                if (s[0].equals("source")) {
                    sources.put(Integer.valueOf(s[2]), MembershipList.memberslist.get(Integer.valueOf(s[1])));
                } else if (s[0].equals("op1")) {
                    op1s.put(Integer.valueOf(s[2]), MembershipList.memberslist.get(Integer.valueOf(s[1])));
                } else {
                    op2s.put(Integer.valueOf(s[2]), MembershipList.memberslist.get(Integer.valueOf(s[1])));
                }
                this.receiverPorts.put(Integer.valueOf(s[2]), Integer.valueOf(s[3]));
            }
        }
        Iterator<Map.Entry<Integer,Member>> iterator = op1s.entrySet().iterator();
        int tempId = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer,Member> entry = iterator.next();
            partMapOp1.put(tempId, entry.getKey());
            tempId++;
        }

        iterator = op2s.entrySet().iterator();
        tempId = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer,Member> entry = iterator.next();
            partMapOp2.put(tempId, entry.getKey());
            tempId++;
        }

        logList.add("Logs are for worker id : " + selfId);
        saveLog(logList);
        sendLog("CREATE");
    }

    //TODO Function : Source
    public void source(){
        //TODO send the data to members present in op1 based on a partition function
        //TODO put the below code in a another jar file
        //TODO Get tuples from the code in object
        //------------------------------
        try {
            Iterator<Map.Entry<Integer,Member>> iterator = op1s.entrySet().iterator();
            String[] range = ranges.split(",");
            int startLine = Integer.parseInt(range[0]);
            int endLine = Integer.parseInt(range[1]);
            Sender sender = new Sender();
            try (BufferedReader reader = new BufferedReader(new FileReader(filename));
                 ObjectOutputStream oos = new ObjectOutputStream(System.out)) {
                String line;
                int lineNumber = 0;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (lineNumber >= startLine && lineNumber <= endLine) {
                        //Send the line in a form of tuple to next node and create a Tuple object for each line
                        String tupleKey = String.valueOf(lineNumber);
                        Tuple<String,String> tuple = new Tuple<>(tupleKey,tupleKey, line);
//                        Map.Entry<Integer, Member> entry = iterator.next();
//                        int workerId = entry.getKey();
//                        Member member = entry.getValue();
                        int hash = generateHashInRange(tuple.getKey(), op1s.size());
                        int workerId = partMapOp1.get(hash);
                        Member member = op1s.get(workerId);
                        //TODO get a successful or unsucessful message from the sender
                        //if it is unsuccessful then it means the node has failed and remove it from the list
                        sender.sendTuple(tuple, member, workerId, selfId);
                        tuplesSent.add(new QueueData(selfId, workerId, member, tuple, "tuple", tuple.getId()));
//                        System.out.println("Member : " + member.getName() + " " + workerId);
//                        if(!iterator.hasNext()){
//                            iterator = op1s.entrySet().iterator();
//                        }
//                        System.out.println(line);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //TODO Function : Split
    public void op1() throws Exception {
        try {
            while (true) {
                QueueData queueData = WorkerManager.consumerQueues.get(selfId).take();
                if(queueData.type.equals("tuple")) {
                    //TODO get the code processed from the executor
                    OperationExecutor.set("Filter");
                    OperationExecutor.loadInstance();
                    System.out.println("Got Tuple id : " + queueData.tupleId);
                    Map<String,String> result = (Map<String,String>)
                            OperationExecutor.executeCode(
                                    queueData.tuple,
                                    "Streetname");
                    Iterator<Map.Entry<String, String>> iterator = result.entrySet().iterator();
                    int count = 0;
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> entry = iterator.next();
                        Tuple<String, String> tuple = new Tuple(queueData.tupleId + "_" + count, entry.getKey(), entry.getValue());
                        count++;
                        int hash = generateHashInRange(tuple.getKey(), op2s.size());
                        int workerId = partMapOp2.get(hash);
                        Member member = op2s.get(workerId);
                        //TODO get a successful or unsucessful message from the sender
                        //if it is unsuccessful then it means the node has failed, so just put a log that unable to send to __ worker
                        String response = sender.sendTuple(tuple, member, workerId, selfId);
                        tuplesSent.add(new QueueData(selfId, workerId, member, tuple, "tuple", tuple.getId()));
                        logList.add(queueData.tuple.getId() + "_Processed");
                        //Save and send the logs to HyDFS
                        saveLog(logList);
                        logList.clear();
                        sendLog("APPEND");
                        System.out.println("Member : " + member.getName() + " " + workerId);
                        sender.sendAckToParent(queueData.tuple.getId(), queueData.member, queueData.senderWorkerId, selfId);
                        logList.add(queueData.tuple.getId() + "_Ack Sent");
                    }
                }else{
                    tuplesSent.removeIf(qd -> qd.tupleId.equals(queueData.tupleId));
                    System.out.println("Received Ack");
                }
            }
        }catch (Exception e){
            System.out.println("Worker failed while processing OP1 : " + selfId);
            e.printStackTrace();
        }
    }

    //TODO Function : Count
    public void op2() throws Exception {
        try {
            while (true) {
                QueueData queueData = WorkerManager.consumerQueues.get(selfId).take();
                if(queueData.type.equals("tuple")) {
                    System.out.println("Got Tuple id : " + queueData.tupleId);
                    //TODO get the code processed from the executor
                    OperationExecutor.set("ExtractColumns");
                    OperationExecutor.loadInstance();
                    Map<String,String> result = (Map<String,String>)
                            OperationExecutor.executeCode(
                                    queueData.tuple,
                                    "Streetname"
                            );
                    Iterator<Map.Entry<String, String>> iterator = result.entrySet().iterator();
                    int count = 0;
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> entry = iterator.next();
                        Tuple<String, String> tuple = new Tuple(queueData.tupleId + "_" + count, entry.getKey(), entry.getValue());
                        logList.add(queueData.tuple.getId() + "_Ack Sent");
                        logList.add(queueData.tuple.getId() + "_Processed");
                        //Save and send the logs to HyDFS
                        saveLog(logList);
                        sendLog("APPEND");
                        logList.clear();
                        //                    //Send the Data to HyDFS
                        //                    sendData();
                        //send data to Introducer
                        sender.sendTupleToLeader(String.valueOf(tuple.getValue()), WorkerManager.leader, selfId);
                        sender.sendAckToParent(queueData.tuple.getId(), queueData.member, queueData.senderWorkerId, selfId);
                    }
                }else{
                    tuplesSent.removeIf(qd -> qd.tupleId.equals(queueData.tupleId));
                    System.out.println("Received Ack");
                }
            }
        }catch (Exception e){
            System.out.println("Worker failed while processing OP1 : " + selfId);
            e.printStackTrace();
        }
    }

    public String saveLog(List<String> logs) {
        // Create filename in the format "WorkerID_type.log"
        String logFileName = "HyDFS\\" + selfId + "_" + type +"_log.txt";

        try {
            // Create a File object with the filename
            File logFile = new File(logFileName);

            // Create FileWriter with false parameter to overwrite existing file
            try (FileWriter writer = new FileWriter(logFile, false)) {
                // Iterate through the logs and write each log entry to the file
                for (String log : logs) {
                    writer.write(log + System.lineSeparator());
                }
            }

            // Return the absolute path of the created file
            return logFile.getAbsolutePath();
        } catch (IOException e) {
            // Handle any potential IO exceptions
            System.err.println("Error writing logs to file: " + e.getMessage());
            return "";
        }
    }

    public void sendLog(String filetype){
        String FileName = "HyDFS\\" + selfId + "_" + type +"_log.txt";
        String HyDFSFileName = selfId + "_" + type +"_log.txt";
        try {
            if(filetype.equals("APPEND")) {
                int fileNameHash = HashFunction.hash(HyDFSFileName);
                Member member = MembershipList.getMemberById(fileNameHash);
                FileSender fileSender = new FileSender(
                        FileName,
                        HyDFSFileName,
                        member.getIpAddress(),
                        Integer.parseInt(member.getPort()),
                        "UPLOAD",
                        "APPEND",
                        "");
                fileSender.run();
            }else{
                int fileNameHash = HashFunction.hash(HyDFSFileName);
                Member member = MembershipList.getMemberById(fileNameHash);
                FileSender fileSender = new FileSender(
                        FileName,
                        HyDFSFileName,
                        member.getIpAddress(),
                        Integer.parseInt(member.getPort()),
                        "UPLOAD",
                        "CREATE",
                        "");
                fileSender.run();
            }
        } catch (RuntimeException e) {
            System.out.println("File Append was unsuccessful");
        }
    }

    public void sendData(){
        String FileName = selfId + "_" + type +".ser";
        String HyDFSFileName = selfId + "_" + type +".ser";
        try {
            int fileNameHash = HashFunction.hash(HyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            FileSender fileSender = new FileSender(
                    FileName,
                    HyDFSFileName,
                    member.getIpAddress(),
                    Integer.parseInt(member.getPort()),
                    "UPLOAD",
                    "APPEND",
                    "");
            fileSender.run();
        } catch (RuntimeException e) {
            System.out.println("File Append was unsuccessful");
        }
    }

    public int generateHashInRange(String input, int range) {
        if (range <= 0) {
            throw new IllegalArgumentException("Range must be a positive number.");
        }
        int hash = input.hashCode();
        hash = Math.abs(hash);
        return hash % range;
    }

    //TODO Function : run function for the thread
    public void run(){
        switch(type) {
            case "source":
                System.out.println("starting source");
                source();
                break;
            case "op1":
                try {
                    System.out.println("starting op1");
                    op1();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "op2":
                try {
                    System.out.println("starting op2");
                    op2();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
        }


    }
}
