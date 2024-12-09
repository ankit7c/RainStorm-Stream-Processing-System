package org.example.Stream;

import org.example.Executor.OperationExecutor;
import org.example.FileSystem.FileSender;
import org.example.FileSystem.HashFunction;
import org.example.FileSystem.Sender;
import org.example.entities.Member;
import org.example.entities.MembershipList;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

//TODO based on role a Worker thread will be created and a specific function will be called
public class Worker implements Runnable {
    public Thread thread;
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
    String pattern;
    //Worker ID, Receiver Port
    HashMap<Integer, Integer> receiverPorts = new HashMap<>();
    HashMap<Integer, Integer> partMapOp1 = new HashMap<>();
    HashMap<Integer, Integer> partMapOp2 = new HashMap<>();
    private Sender sender = new Sender();

    public List<QueueData> tuplesSent = new CopyOnWriteArrayList<>();
    public List<String> logList = new CopyOnWriteArrayList<>();
    public HashSet<String> tuplesReceived = new HashSet<>();

    public int receiverPort;
    private static volatile boolean running = true;
    public int EOFs = 0;

    public void stopWorker(){
        System.out.println("Stopping worker");
        running = false;
    }

    public void restartWorker(){
        System.out.println("Restarting worker");
        running = true;
    }

    public Worker(String type, List<Member> source , List<Member> op1, List<Member> op2, String ranges, String filename, String destFileName, String operationName, String patten) {
        this.source = source;
        this.op1 = op1;
        this.op2 = op2;
        this.ranges = ranges;
        this.type = type;
        this.filename = filename;
        this.destFileName = destFileName;
        this.operationName = operationName;
        this.pattern = patten;
        running = true;
    }

    public void setReceiverPorts(ArrayList<String> receiverPorts) {
        for (String port : receiverPorts) {
            System.out.println(port);
            if(!port.isBlank()) {
                String[] s = port.split(":");
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
        saveData(List.of(new Tuple<>("1", "Start", "of file")));
        sendData("CREATE");
    }

    public void setTuplesReceived(HashSet<String> tuplesReceived) {
        this.tuplesReceived.addAll(tuplesReceived);
    }

    public void setNewWorker(String type, int newWorkerId, int failedWorkerId, int newWorkerMemberId) {
        EOFs = 0;
        if(newWorkerId == selfId)
            return;
        if(type.equals("op1")) {
            op1s.remove(failedWorkerId);
            if(!op1s.containsKey(newWorkerId))
                op1s.put(newWorkerId, MembershipList.memberslist.get(newWorkerMemberId));
            int key = -1;
            for (Map.Entry<Integer, Integer> entry : partMapOp1.entrySet()) {
                if (entry.getValue() == failedWorkerId) {
                    key = entry.getKey();
                    break;
                }
            }
            if(key != -1){
                partMapOp1.put(key, newWorkerId);
            }
        }else{
            op2s.remove(failedWorkerId);
            if(!op2s.containsKey(newWorkerId))
                op2s.put(newWorkerId, MembershipList.memberslist.get(newWorkerMemberId));
            int key = -1;
            for (Map.Entry<Integer, Integer> entry : partMapOp2.entrySet()) {
                if (entry.getValue() == failedWorkerId) {
                    key = entry.getKey();
                    break;
                }
            }
            if(key != -1) {
                partMapOp2.put(key, newWorkerId);
            }
        }
//        System.out.println("List After setting new worker");
//        op1s.forEach((k,v) -> {
//            System.out.println(k + " : " + v.getName());
//        });
//        op2s.forEach((k,v) -> {
//            System.out.println(k + " : " + v.getName());
//        });
//        partMapOp1.forEach((k,v) -> {
//            System.out.println(k + " : " + v);
//        });
//        partMapOp2.forEach((k,v) -> {
//            System.out.println(k + " : " + v);
//        });
    }

    //TODO Function : Source
    public void source(){
        //TODO send the data to members present in op1 based on a partition function
        //TODO put the below code in a another jar file
        //TODO Get tuples from the code in object
        //------------------------------
        try {
            Iterator<Map.Entry<Integer,Member>> iterator = op1s.entrySet().iterator();
            System.out.println("Range : " + selfId + " : " + ranges);
            String[] range = ranges.split(",");
            int startLine = Integer.parseInt(range[0]);
            int endLine = Integer.parseInt(range[1]);
            Sender sender = new Sender();
            int logLine = 0;
            try (Scanner scanner = new Scanner(new File(filename))) {
                String line;
                int lineNumber = 0;
                while (scanner.hasNextLine() && running) {
                    line = scanner.nextLine();
                    lineNumber++;
                    if (lineNumber >= startLine && lineNumber <= endLine) {
                        //Send the line in a form of tuple to next node and create a Tuple object for each line
                        String tupleKey = String.valueOf(lineNumber);
                        Tuple<String,String> tuple = new Tuple<>(tupleKey,tupleKey, line);
                        int hash = generateHashInRange(tuple.getKey(), op1s.size());
                        int workerId = partMapOp1.get(hash);
                        Member member = op1s.get(workerId);
                        //TODO get a successful or unsucessful message from the sender
                        //if it is unsuccessful then it means the node has failed and remove it from the list
                        String response = sender.sendTuple(tuple, "tuple", member, workerId, selfId);
                        if(response.contains("Unsuccessful")){
                            logList.add(workerId + " : " + tupleKey + " : failed sent");
                            System.out.println(workerId + " : " + tupleKey + " : failed sent");
                        }else{
                            tuplesSent.add(new QueueData(selfId, workerId, member, tuple, "tuple", tuple.getId()));
                            logList.add(workerId + " : " + tupleKey + " : sent");
                            System.out.println(workerId + " : " + tupleKey + " : sent");
                        }
                        while(!WorkerManager.consumerQueues.get(selfId).isEmpty()){
                            QueueData queueData = WorkerManager.consumerQueues.get(selfId).take();
                            tuplesSent.removeIf(qd -> qd.tupleId.equals(queueData.tupleId));
                            logList.add(queueData.senderWorkerId + " : " + queueData.tupleId + " : Ack Received");
                            System.out.println(queueData.senderWorkerId + " : " + queueData.tupleId + " : Ack Received");
                        }
                        if(logLine%100 == 0){
                            logList.add(selfId + " : sending Logs");
                            saveLog(logList);
                            logList.clear();
                            sendLog("APPEND");
                        }
                    }
                    logLine++;
                }
                logList.add(selfId + " : sending Logs");
                saveLog(logList);
                logList.clear();
                sendLog("APPEND");
                while (iterator.hasNext() && running) {
                    Map.Entry<Integer,Member> entry = iterator.next();
                    int workerId = entry.getKey();
                    Member member = entry.getValue();
                    Tuple<String,String> tuple = new Tuple<>(String.valueOf(lineNumber),String.valueOf(lineNumber), "End of file");
                    sender.sendTuple(tuple, "end", member, workerId, selfId);
                    System.out.println("Sending EOF to " + workerId);
                }
                System.out.println("Stopped");
            } catch (IOException e) {
                e.printStackTrace();
            }
            logList.add(selfId + " : sending Logs");
            saveLog(logList);
            logList.clear();
            sendLog("APPEND");
            System.out.println("Stopping worker as process ended");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //TODO Function : Split
    public void op1() throws Exception {
        try {
            OperationExecutor operationExecutor = new OperationExecutor();
            operationExecutor.set(operationName, selfId, type);
            operationExecutor.loadInstance();
            int logLine = -1;
            while (running) {
                QueueData queueData = WorkerManager.consumerQueues.get(selfId).poll(100, TimeUnit.MILLISECONDS);
                logLine++;
                if(queueData != null) {
                    if (queueData.type.equals("tuple")) {
//                        if(tuplesReceived.contains(queueData.tupleId)){
//                            System.out.println("Got Duplicate Tuple id : " + queueData.tupleId);
//                        }
                        logList.add(queueData.senderWorkerId + " : " + queueData.tuple.getId() + " : received");
                        Map<String, String> result = (Map<String, String>)
                                operationExecutor.executeCode(
                                        queueData.tuple,
                                        pattern);
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
                            String response = sender.sendTuple(tuple, "tuple", member, workerId, selfId);
                            if(response.contains("Unsuccessful")){
                                logList.add(workerId + " : " + tuple.getId() + " : failed sent");
                                System.out.println(workerId + " : " + tuple.getId() + " : failed sent");
                            }else {
                                tuplesSent.add(new QueueData(selfId, workerId, member, tuple, "tuple", tuple.getId()));
                                logList.add(workerId + " : " + tuple.getId() + " : sent");
                                System.out.println(workerId + " : " + tuple.getId() + " : sent");
                            }
                            tuplesReceived.add(queueData.tupleId);
                            //Save and send the logs to HyDFS
                            if(logLine%100 == 0) {
                                logList.add(selfId + " : sending Logs");
                                saveLog(logList);
                                sendLog("APPEND");
                                logList.clear();
                            }
                        }
                        sender.sendAckToParent(queueData.tuple.getId(), queueData.member, queueData.senderWorkerId, selfId);
                        logList.add(queueData.senderWorkerId + " : " + queueData.tuple.getId() + " : Ack sent");
                        System.out.println(queueData.senderWorkerId + " : " + queueData.tuple.getId() + " : Ack sent");
                    } else if (queueData.type.equals("ack")) {
                        tuplesSent.removeIf(qd -> qd.tupleId.equals(queueData.tupleId));
                        logList.add(queueData.senderWorkerId + " : " + queueData.tupleId + " : Ack Received");
                        System.out.println(queueData.senderWorkerId + " : " + queueData.tupleId + " : Ack Received");
                    } else {
                        EOFs++;
                        System.out.println();
                        if (EOFs >= sources.size()) {
                            Iterator<Map.Entry<Integer, Member>> iterator = op2s.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<Integer, Member> entry = iterator.next();
                                int workerId = entry.getKey();
                                Member member = entry.getValue();
                                String response = sender.sendTuple(queueData.tuple, "end", member, workerId, selfId);
                                saveLog(logList);
                                sendLog("APPEND");
                                logList.clear();
                            }
                        }
                    }
                }
            }
            logList.add(selfId + " : sending Logs");
            saveLog(logList);
            sendLog("APPEND");
            logList.clear();
            System.out.println("Stopping worker as process ended : " + selfId);
        }catch (Exception e){
            System.out.println("Worker failed while processing OP1 : " + selfId);
            e.printStackTrace();
        }
    }

    //TODO Function : Count
    public void op2() throws Exception {
        try {
            OperationExecutor operationExecutor = new OperationExecutor();
            operationExecutor.set(operationName, selfId, type);
            operationExecutor.loadInstance();
            operationExecutor.loadCode();
            int logLine = 1;
            List<Tuple> tupleList = new ArrayList<>();
            while (running) {
                logLine++;
                QueueData queueData = WorkerManager.consumerQueues.get(selfId).poll(100, TimeUnit.MILLISECONDS);
                if(queueData != null) {
                    if (queueData.type.equals("tuple")) {
                        if(!tuplesReceived.contains(queueData.tuple.getId())) {
                            System.out.println("Got Tuple id : " + queueData.tupleId);
                            logList.add(queueData.senderWorkerId + " : " + queueData.tuple.getId() + " : received");
                            Map<String, String> result = (Map<String, String>)
                                    operationExecutor.executeCode(
                                            queueData.tuple,
                                            pattern
                                    );
                            logList.add("New Tuples : " + result.size() + " : " + queueData.tuple.getValue());
                            Iterator<Map.Entry<String, String>> iterator = result.entrySet().iterator();
                            int count = 0;
                            while (iterator.hasNext()) {
                                Map.Entry<String, String> entry = iterator.next();
                                Tuple<String, String> tuple = new Tuple(queueData.tupleId + "_" + count, entry.getKey(), entry.getValue());
                                logList.add(tuple.getId() + " : processed");
                                System.out.println(tuple.getId() + " : processed");
                                //Save and send the logs to HyDFS
                                tupleList.add(tuple);
                                tuplesReceived.add(queueData.tuple.getId());
                                if (logLine % 100 == 0) {
                                    logList.add(selfId + " : sending Logs and state");
                                    saveLog(logList);
                                    saveData(tupleList);
                                    sendLog("APPEND");
                                    logList.clear();
                                    tupleList.clear();
                                    //Send the Data to HyDFS
                                    operationExecutor.saveCode();
                                    sendData("APPEND");
                                    sendState();
                                }
                                //send data to Introducer
                                sender.sendTupleToLeader(selfId + " : " + tuple.getKey() + " : " + tuple.getValue(), WorkerManager.leader, selfId);
                            }
                            sender.sendAckToParent(queueData.tuple.getId(), queueData.member, queueData.senderWorkerId, selfId);
                            logList.add(queueData.senderWorkerId + " : " + queueData.tuple.getId() + " : Ack sent");
                        }else{
                            System.out.println("Got Duplicate Tuple id : " + queueData.tupleId);
                        }
                    } else if (queueData.type.equals("ack")) {
                        tuplesSent.removeIf(qd -> qd.tupleId.equals(queueData.tupleId));
                        logList.add(queueData.tupleId + " Ack Received");
                        System.out.println("Received Ack");
                    } else {
                        EOFs++;
                        if (EOFs >= op1s.size()) {
                            sender.sendEOFToLeader(WorkerManager.leader, selfId);
                            System.out.println("Sending EOF to leader");
                            saveLog(logList);
                            sendLog("APPEND");
                            saveData(tupleList);
                            logList.clear();
                            tupleList.clear();
                            //Send the Data to HyDFS
                            sendData("APPEND");
                            sendState();
                        }
                    }
                }
            }
            saveLog(logList);
            sendLog("APPEND");
            logList.clear();
            sendState();
            System.out.println("Stopping worker as process ended");
        }catch (Exception e){
            System.out.println("Worker failed while processing OP1 : " + selfId);
            e.printStackTrace();
        }
    }

    public String saveLog(List<String> logs) {
        // Create filename in the format "WorkerID_type.log"
        String logFileName = "local\\" + selfId + "_" + type +"_log.txt";
        try {
            File logFile = new File(logFileName);
            try (FileWriter writer = new FileWriter(logFile, false)) {
                for (String log : logs) {
                    writer.write(log + System.lineSeparator());
                }
            }
            return logFile.getAbsolutePath();
        } catch (IOException e) {
            System.err.println("Error writing logs to file: " + e.getMessage());
            return "";
        }
    }

    public String saveData(List<Tuple> tupleList) {
        // Create filename in the format "WorkerID_type.log"
        String logFileName = "local\\" + selfId + "_" + type +"_data.txt";
        try {
            File logFile = new File(logFileName);
            try (FileWriter writer = new FileWriter(logFile, false)) {
                int size = tupleList.size();
                for(int i = 0; i < size; i++) {
                    writer.write(tupleList.get(i).getId() + " : " + tupleList.get(i).getKey() + " : " + tupleList.get(i).getValue() + System.lineSeparator());
                }
            }
            return logFile.getAbsolutePath();
        } catch (IOException e) {
            System.err.println("Error writing logs to file: " + e.getMessage());
            return "";
        }
    }

    public void sendLog(String filetype){
        String FileName = "local\\" + selfId + "_" + type +"_log.txt";
        String HyDFSFileName = selfId + "_" + type +"_log.txt";
        System.out.println("Sending log : " + HyDFSFileName);
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
            System.out.println("log sent successfully");
        } catch (RuntimeException e) {
            System.out.println("File Append was unsuccessful");
        }
    }

    public void sendData(String filetype){
        String FileName = "local\\" + selfId + "_" + type +"_data.txt";
        String HyDFSFileName = destFileName;
        System.out.println("Sending data : " + FileName);
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
                System.out.println("Sending file");
                fileSender.run();
                System.out.println("File Sent");
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

    public void sendState(){
        String FileName = selfId + "_" + type +"_ser.ser";
        String HyDFSFileName = selfId + "_" + type +"_ser.ser";
        System.out.println("Sending data : " + HyDFSFileName);
        Path path = Paths.get(FileName);
        if(Files.exists(path)) {
            try {
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
            } catch (RuntimeException e) {
                System.out.println("File Append was unsuccessful");
            }
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
