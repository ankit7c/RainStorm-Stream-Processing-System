package org.example.Stream;

import org.example.Executor.OperationExecutor;
import org.example.FileSystem.FileSender;
import org.example.FileSystem.HashFunction;
import org.example.FileSystem.Sender;
import org.example.entities.Member;
import org.example.entities.MembershipList;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
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
    private Sender sender = new Sender();
    private StreamSender streamSender = new StreamSender(this) ;
    private StreamReceiver streamReceiver = new StreamReceiver(this);

    int batchId = 0;
    int tupleId = 0;
    public  List<Batch> batchesSent = new CopyOnWriteArrayList<>();
    public  List<Batch> batchesToBeSent= new CopyOnWriteArrayList<>();
    public  List<Batch> batchesReceived = new CopyOnWriteArrayList<>();
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
            if(!iterator.hasNext()){
                iterator = op1s.entrySet().iterator();
            }
            Map.Entry<Integer,Member>entry = iterator.next();
            Batch batch = new Batch(String.valueOf(batchId), selfId, entry.getKey(), null);
            try (Scanner scanner = new Scanner(new File(filename))) {
                int currentLine = 0;
                while (scanner.hasNextLine()) {
                    currentLine++;
                    String line = scanner.nextLine();
                    if (currentLine >= startLine && currentLine <= endLine) {
                        //TODO put the line in a form of tuple in the queue
                        batch.getBatchData().add(new Tuple(String.valueOf(tupleId++), currentLine, line));
                       // System.out.println(line);
                    }
                    if (currentLine > endLine) {
                        break; // Stop reading once we've passed the desired range
                    }
                  if(batch.getBatchData().size() >= 10){
                        batchesToBeSent.add(batch);
                        if(!iterator.hasNext()){
                          iterator = op1s.entrySet().iterator();
                        }
                        entry = iterator.next();
                        batch = new Batch(String.valueOf(batchId), selfId, entry.getKey(),null);
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        //TODO Pass the tuple to the queue which will send it to next nodes.
        //------------------------------
    }

    //TODO Function : Split
    public void op1() throws Exception {
        //TODO based on num tasks create a connection
        for(Batch currBatch: batchesReceived){
            List<String> linesFromBatch = new CopyOnWriteArrayList<>();
            String batchId = currBatch.getBatchId();
            List<Tuple> batchData = currBatch.getBatchData();

            linesFromBatch = batchData.stream()
                    .map(Tuple::getValue)
                    .filter(value -> value != null)
                    .map(Object::toString)
                    .collect(Collectors.toList());

            OperationExecutor.set("op1");
            OperationExecutor.loadInstance();
            // Function to process data
            List<String> result = (List<String>) OperationExecutor.executeCode(linesFromBatch);

            int totalBatches = op2.size();
            List<Batch>op2Batches = new CopyOnWriteArrayList<>();

            int count = 1;
            for(Map.Entry<Integer,Member>entry : op2s.entrySet()){
                op2Batches.add(new Batch(batchId+"_"+count++, selfId, entry.getKey() , new CopyOnWriteArrayList<>()));
            }
            for(String word:result){
                int machine = getMachine(word,op2Batches.size());
                Tuple newTuple = new Tuple(UUID.randomUUID().toString(), word, 1);
                op2Batches.get(machine).getBatchData().add(newTuple);
            }

            batchesToBeSent.addAll(op2Batches);
            String parentIp = getAckReceiverIP(currBatch.getSenderWorkerId());
            sender.sendAckToParent(parentIp,receiverPorts.get(currBatch.getSenderWorkerId()),currBatch.getSenderWorkerId(), currBatch.getBatchId());
            logList.add(currBatch.getBatchId()+"_Ack Sent");
            logList.add(currBatch.getBatchId()+"_Processed");
            batchesReceived.remove(currBatch);

        }
    }

    private String getAckReceiverIP(int parentMachineId){
        String receiverIp = "";
        if(op1s.containsKey(parentMachineId)){
            Member member = op1s.get(parentMachineId);
            receiverIp = member.getIpAddress();
        }else if(sources.containsKey(parentMachineId)){
            Member member = sources.get(parentMachineId);
            receiverIp = member.getIpAddress();
        }
        return receiverIp;
    }

    public static int getMachine(String word, int n) {
        char firstChar = Character.toLowerCase(word.charAt(0));
        int machine = (firstChar - 'a') * n / 26;
        return machine;
    }

    //TODO Function : Count
    public void op2() throws Exception {
        List<String> wordsList = new CopyOnWriteArrayList<>();
        for(Batch currBatch: batchesReceived){
            List<Tuple> batchData = currBatch.getBatchData();
            for(Tuple currTuple: batchData){
                wordsList.add(String.valueOf(currTuple.getKey()));
            }
            String parentIp = getAckReceiverIP(currBatch.getSenderWorkerId());
            sender.sendAckToParent(parentIp,receiverPorts.get(currBatch.getSenderWorkerId()),currBatch.getSenderWorkerId(), currBatch.getBatchId());
            batchesReceived.remove(currBatch);
        }

        OperationExecutor.set("op2");
        OperationExecutor.loadInstance();
        OperationExecutor.loadCode();
        Map<String, Long> result = (Map<String, Long>) OperationExecutor.executeCode(wordsList);
        //Save and send the logs to HyDFS
        saveLog(logList);
        sendLog();
        //Send the Data to HyDFS
        sendData();
    }

    public void sendBatchData(){

    }

    public void receiveBatchData(){

    }

    public void processop1(List<Tuple>l1){

    }

    public void processAck(String batchId){
        batchesSent.removeIf(batch -> batch.getBatchId().equals(batchId));
    }

    private void processBatches(int OperationStage) throws Exception {
            for(Batch currBatch : batchesReceived){
                //TODO perform some operation
                if(OperationStage==1){
                    //TODO perform some operation & build new tuples data
                    List<Tuple> tuplesForNextStage = new CopyOnWriteArrayList<>();
                    //TODO decide batch id currentl giving random
                    Batch nextStageBatch = new Batch("1", selfId,tuplesForNextStage);
                    batchesToBeSent.add(nextStageBatch);
                    //TODO decide how we are going to send batches , from here or from run of sender

                } else if(OperationStage == 2){
                    //TODO perform operstions and write to Console and HYDFS



                    //sender.sendAckToParent(currBatch.getSenderWorkerId(),currBatch.getBatchId());
                    //TODO Need to write sendack & receive ack , but where ?
                }

            }
    }

    public int getReceiverPort() {
        return receiverPort;
    }
    public String saveLog(List<String> logs) {
        // Create filename in the format "WorkerID_type.log"
        String logFileName = selfId + "_" + type +"_log.txt";

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

    public void sendLog(){
        String FileName = selfId + "_" + type +"_log.txt";
        String HyDFSFileName = selfId + "_" + type +"_log.txt";
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

    public void setReceiverPort(int receiverPort) {
        this.receiverPort = receiverPort;
    }
    //TODO Function : run function for the thread
    public void run(){
        streamReceiver.start();
        streamSender.start();
        switch(type) {
            case "source":
                source();
                break;
            case "op1":
                try {
                    op1();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "op2":
                try {
                    op2();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
        }


    }
}
