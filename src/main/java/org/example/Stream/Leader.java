package org.example.Stream;

import org.example.FileSystem.HashFunction;
import org.example.FileSystem.HelperFunctions;
import org.example.FileSystem.Sender;
import org.example.entities.FDProperties;
import org.example.entities.Member;
import org.example.entities.MembershipList;
import org.example.entities.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Leader {
    public static class WorkerTasks {
        public String type;
        Member member;
        Integer receiverPort;
        Integer workerId;
        ConcurrentSkipListMap<Integer, Member> memberslist;
        public WorkerTasks(String type, Member member, int workerId, Integer receiverPort) {
            this.type = type;
            this.member = member;
            this.receiverPort = receiverPort;
            this.workerId = workerId;
        }
    }
    public static class Range{
        public int start;
        public int end;
        public Range(int start, int end){
            this.start = start;
            this.end = end;
        }
    }
    static LocalDateTime startTime;
    static List<Member> sources = new ArrayList<>();
    static List<Member> op1 = new ArrayList<>();
    static List<Member> op2 = new ArrayList<>();
    static List<String> ranges = new ArrayList<>();
    static HashMap<Integer ,Range> sourceRange = new HashMap<>();
    static ConcurrentSkipListMap<Integer, WorkerTasks> workerIds = new ConcurrentSkipListMap<>();
    static int pointer = 0;
    static Sender sender = new Sender();
    static String currFilename;
    static String currDestFilename;
    static String operation1Name;
    static String operation2Name;
    static String patternop1;
    static String patternop2;
    static ConcurrentSkipListMap<Integer,Integer> totalEOFs = new ConcurrentSkipListMap<>();
    public static List<Integer> failedNodes = new CopyOnWriteArrayList<>();
    public static List<Integer> failedWorkerIds = new CopyOnWriteArrayList<>();
    //Add functions to send  control commands to worker nodes

    //Function to determine which nodes are active and assign tasks to each of them
    public static void initializeNodes(String filename,
                                String destFilename, int num_tasks, String[] ops, String pattern1, String pattern2) {

        startTime = LocalDateTime.now();
        ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
        //Remove itself
        memberslist.remove(MembershipList.selfId);
        ArrayList<Integer> ids = new ArrayList<>();
        memberslist.forEach((k,v) -> ids.add(k));
        patternop1 = pattern1;
        patternop2 = pattern2;
        int size = memberslist.size();
        try {
            long line = HelperFunctions.countLines(filename);
            long start = 0;
            for(int i = 0; i < num_tasks; i++){
                String range = ((i == num_tasks-1) ? (start + "," + line) : (start + "," + (start + (line / num_tasks))));
                start = start + (line / num_tasks) + 1;
                ranges.add(range);
                System.out.println("Range: " + range);
                System.out.println("id : " + ids.get(pointer%size));
                sources.add(memberslist.get(ids.get(pointer%size)));
                pointer++;
            }
            for(int i = 0; i < num_tasks; i++){
                System.out.println("id : " + ids.get(pointer%size));
                op1.add(memberslist.get(ids.get(pointer%size)));
                pointer++;
            }
            for(int i = 0; i < num_tasks; i++){
                System.out.println("id : " + ids.get(pointer%size));
                op2.add(memberslist.get(ids.get(pointer%size)));
                pointer++;
            }
            //Call each node and assign the role
            Map<WorkerTasks, String> result = sender.setRoles(sources, op1, op2, filename, ranges, destFilename, ops, pattern1, pattern2);
            operation1Name = ops[0];
            operation2Name = ops[1];
                    //Check if each result is pass, if not then pick new node and ask it to handle the task
            result.forEach(((workerTasks, s) -> {
                System.out.println("Adding members :" + Integer.valueOf(s));
                workerIds.put(Integer.valueOf(s),workerTasks);
                if(workerTasks.type.equals("source")){
                    int index = sources.indexOf(workerTasks.member);
                    String range = ranges.get(index);
                    int st = Integer.parseInt(range.split(",")[0]);
                    int end = Integer.parseInt(range.split(",")[1]);
                    sourceRange.put(workerTasks.workerId, new Range(st,end));
                }
            }));

            //Send the Receiver ports to all workers
            ArrayList<String> receiverPorts = new ArrayList<>();
            workerIds.forEach((workerId,workerTask) -> {
                //Along with worker id send the data
                receiverPorts.add(workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort);
                System.out.println(workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort);
            });
            workerIds.forEach((workerId,workerTask) -> {
                Member member = memberslist.get(workerTask.member.getId());
                sender.startProcessing(member, workerId, receiverPorts);
            });

            pointer = pointer%size;
            currFilename = filename;
            currDestFilename = destFilename;

            System.out.println("Starting cf");
            checkFailure cf = new checkFailure();
            cf.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void addFailedNodes(int Failedid){
        failedNodes.add(Failedid);
        for (Map.Entry<Integer, WorkerTasks> entry : workerIds.entrySet()) {
            Integer workerId = entry.getKey();
            WorkerTasks value = entry.getValue();
            if (value.member.getId() == Failedid) {
                if(value.type.equals("op2")) {
                    failedWorkerIds.add(workerId);
                }
            }
        }
    }

    public static void processEOFs(int workerId){
        totalEOFs.put(workerId,1);
        System.out.println("Recieved EOF from : " + workerId);
        if(totalEOFs.size() == op2.size()){
            try {
                LocalDateTime endTime = LocalDateTime.now();
                System.out.println("End Time: " + endTime);
                Duration duration = Duration.between(startTime, endTime);
                long seconds = duration.toSeconds();
                System.out.println("Time taken: " + seconds + " seconds");
                //All processes have been ended, say worker managers to kill the nodes
                Thread.sleep(5000);
                workerIds.forEach((k, v) -> {
                    System.out.println("Killing worker " + k);
                    sender.killWorkers(v.member, k);
                });
                //Code to clear all the rainstorm data
                System.out.println("Clearing all caches");
                sources.clear();
                op1.clear();
                op2.clear();
                ranges.clear();
                workerIds.clear();
                pointer = 0;
                totalEOFs.clear();
                sourceRange.clear();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    //TODO Function to take action when a node is failed
    public static void updateFailedNode(){
        //Get the failed members Worker Ids
//        Member failMember = null;
        totalEOFs.clear();
        for (int i = 0; i < failedNodes.size(); i++) {
            int failMemberId = failedNodes.get(i);
            ArrayList<Integer> Failedids = new ArrayList<>();
            for (Map.Entry<Integer, WorkerTasks> entry : workerIds.entrySet()) {
                Integer workerId = entry.getKey();
                WorkerTasks value = entry.getValue();
                if (value.member.getId() == failMemberId) {
                    Failedids.add(workerId);
                }
            }
            System.out.println("Fail member" + failMemberId);
            for(Integer failedId : Failedids){
                System.out.println("Failed ids" + failedId);
            }
            HashMap<Integer, WorkerTasks> FailedTasks = new HashMap<>();
            for(Integer failedId : Failedids) {
                FailedTasks.put(failedId, workerIds.get(failedId));
                workerIds.remove(failedId);
            }
            for(Integer failedId : Failedids){
                WorkerTasks workerTask = FailedTasks.get(failedId);
                String type = workerTask.type;
                //Get the next free node from the list
                //TODO Don't include the failed member here. The failed member should be removed firs to start this process
                ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
                //Remove itself
                MembershipList.memberslist.forEach((k, v) -> System.out.println(k + "," + v.getName()));
                memberslist.remove(MembershipList.selfId);
                ArrayList<Integer> ids = new ArrayList<>();
                memberslist.forEach((k,v) -> ids.add(k));
                pointer++;
                System.out.println("Pointer : " + pointer);
                Member member = memberslist.get(pointer%memberslist.size());
                pointer++;
                //If we choose the failed Member again
                while(member == null){
                    member = memberslist.get(pointer%memberslist.size());
                    pointer++;
                }
                System.out.println("New Machine to replace " + failMemberId + " is " + member.getId());
                //TODO Get the log of the failed node
                //TODO determine the new ranges
                //TODO Ask the new node to set up the role
                //TODO If aggregator ask it to load ser + log
                switch (type){
                    case "source":
                        break;
                    case "op1": {
                        System.out.println("Operation name replaying " + operation1Name);
                        Map<WorkerTasks, String> result = sender.setFailedOp1(member, sources, op2, operation1Name, patternop1, workerTask.workerId);
                        result.forEach(((workerTasks, s) -> {
                            System.out.println("Adding members :" + Integer.valueOf(s));
                            workerIds.put(Integer.valueOf(s), workerTasks);
                        }));
                        Iterator<Map.Entry<WorkerTasks,String>> iterator = result.entrySet().iterator();
                        Map.Entry<WorkerTasks, String> entry = iterator.next();
                        workerTask = entry.getKey();
                        ArrayList<String> receiverPorts = new ArrayList<>();
                        workerIds.forEach((workerId, workerTasks) -> {
                            //Along with worker id send the data
                            receiverPorts.add(workerTasks.type + ":" + workerTasks.member.getId() + ":" + workerTasks.workerId + ":" + workerTasks.receiverPort);
                        });
                        sender.startProcessing(member, workerTask.workerId, receiverPorts);

                        String newWorkerIDData = workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort;
                        int newMemberId = member.getId();
                        System.out.println("newWorkerIDData : " + newWorkerIDData);
                        workerIds.forEach(((workerId, workerTasks) -> {
                            Member tempmember = memberslist.get(workerTasks.member.getId());
                            System.out.println("sending new data :" + tempmember.getId() + " : " + workerId);
                            sender.sendNewNodeData(tempmember, workerId, failedId, newWorkerIDData, newMemberId);
                        }));
                    }
                    break;
                    case "op2": {
                        System.out.println("Operation name replaying " + operation2Name);
                        Map<WorkerTasks, String> result = sender.setFailedOp2(member, op1, currDestFilename, operation2Name, patternop2, workerTask.workerId);
                        result.forEach(((workerTasks, s) -> {
                            System.out.println("Adding members :" + Integer.valueOf(s));
                            workerIds.put(Integer.valueOf(s), workerTasks);
                        }));
                        Iterator<Map.Entry<WorkerTasks,String>> iterator = result.entrySet().iterator();
                        Map.Entry<WorkerTasks, String> entry = iterator.next();
                        workerTask = entry.getKey();
                        ArrayList<String> receiverPorts = new ArrayList<>();
                        workerIds.forEach((workerId, workerTasks) -> {
                            //Along with worker id send the data
                            receiverPorts.add(workerTasks.type + ":" + workerTasks.member.getId() + ":" + workerTasks.workerId + ":" + workerTasks.receiverPort);
                        });
                        sender.startProcessing(member, workerTask.workerId, receiverPorts);
                        //Ask the nodes to replace the failed node in their lists
                        //Ask source to resend the data based on ranges calculated earlier
                        String newWorkerIDData = workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort;
                        System.out.println("newWorkerIDData : " + newWorkerIDData);
                        int newMemberId = member.getId();
                        workerIds.forEach(((workerId, workerTasks) -> {
                            Member tempmember = memberslist.get(workerTasks.member.getId());
                            System.out.println("sending new data :" + tempmember.getId() + " : " + workerId);
                            sender.sendNewNodeData(tempmember, workerId, failedId, newWorkerIDData, newMemberId);
                        }));
                    }
                    break;
                    default:
                        System.out.println("Invalid type, Not able to found the failed node in the current working nodes list");
                        break;
                }
                getLogs(failedId, type);
//                HashMap<Integer ,Integer> rangeMap = getNewRanges(failedId, type);
//                HashMap<Integer ,Integer> rangeMap = new HashMap<>();
//                sourceRange.forEach((k,v) -> {
//                    rangeMap.put(k, v.start);
//                });
//                rangeMap.forEach((sid, lineNo)->{
//                    sender.resendLines(workerIds.get(sid).member, sid, lineNo);
//                });
            }
        }
        if(!failedNodes.isEmpty()) {
//            HashMap<Integer, Integer> rangeMap = new HashMap<>();
//            sourceRange.forEach((k, v) -> {
//                rangeMap.put(k, v.start);
//            });
//            rangeMap.forEach((sid, lineNo) -> {
//                sender.resendLines(workerIds.get(sid).member, sid, lineNo);
//            });
            sourceRange.forEach((k, v) -> {
                System.out.println("Starting resource : " + workerIds.get(k).member.getId() + " " +  k + " " + v.start);
                sender.resendLines(workerIds.get(k).member, k, v.start );
            });
            failedNodes.clear();
            failedWorkerIds.clear();
        }
    }

    public static void getLogs(int failedWorkerId, String type){
        String fileName = failedWorkerId + "_" + type +"_log.txt";
        System.out.println( "fileName" + fileName);
//        sender.get_File("local\\"+fileName,fileName);
//        sender.get_File(fileName,fileName);
        int fileNameHash = HashFunction.hash(fileName);
        try {
            Iterator<Map.Entry<Integer, Member>> iterator = MembershipList.memberslist.entrySet().iterator();
            String response = "Unsuccessful";
            while(iterator.hasNext()) {
                Map.Entry<Integer, Member> entry = iterator.next();
                Member member = entry.getValue();
                if(member.getId() == MembershipList.selfId){
                    entry = iterator.next();
                    member = entry.getValue();
                }
                String IpAddress = member.getIpAddress();
                String port = member.getPort();
                int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
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
                System.out.println("Response : " + response);
                if(response.equals("Successful")) {
                    System.out.println("Log found successfully");
                    break;
                }else {
                    Thread.sleep(2000);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static HashMap<Integer ,Integer> getNewRanges(int failedWorkerId, String type){
        String fileName = "local\\" + failedWorkerId + "_" + type +"_log.txt";
        HashMap<Integer ,Integer> rangeMap = new HashMap<>();
        List<String> lines = readFileReverse(fileName);
        System.out.println("Read Lines : " + lines.size());
        sourceRange.forEach((k,v) ->{
            System.out.println(k + " " + v.start + " " + v.end);
        });
        if(type.equals("op1")) {
            //TODO populate the Map with key as source worker Id
            for (int i = lines.size() - 1; i >= 0; i--) {
                String line = lines.get(i);
                if (line.contains("received")) {
                    String[] temp = line.split(":");
                    String senderId = temp[0].replace(" ", "");
                    int lineNo = Integer.parseInt(temp[1].replace(" ", ""));
                    sourceRange.forEach((id, range) -> {
                        if(lineNo >= range.start && lineNo <= range.end) {
                            rangeMap.put(Integer.valueOf(senderId), Math.max(lineNo, rangeMap.get(Integer.parseInt(senderId))));
                        }
                    });
                }
                if(rangeMap.size() == sources.size()){
                    break;
                }
            }
        }else{
            ArrayList<Integer> sourcesId = new ArrayList<>();
            ArrayList<Integer> op1sId = new ArrayList<>();
            workerIds.forEach((workerId, workerTask) -> {
                if(workerTask.type.equals("source")){
                    sourcesId.add(workerId);
                }else if(workerTask.type.equals("op1")){
                    op1sId.add(workerId);
                }
            });
            for(int oid : op1sId){
                String op1id = String.valueOf(oid);
                HashMap<Integer ,Integer> rangeMapOp1 = new HashMap<>();
                for (int i = lines.size() - 1; i >= 0; i--) {
                    String line = lines.get(i);
                    if (line.contains("received") && line.contains(op1id)) {
                        String[] temp = line.split(":");
                        String senderId = temp[0].replace(" ", "");
                        int lineNo = Integer.parseInt(temp[1].replace(" ", ""));
                        sourceRange.forEach((id, range) -> {
                            if(lineNo >= range.start && lineNo <= range.end) {
                                rangeMapOp1.put(id, Math.max(lineNo, rangeMap.get(id)));
                            }
                        });
                    }
                    if(rangeMap.size() == sources.size()){
                        break;
                    }
                }
                rangeMapOp1.forEach((id, range) -> {
                    rangeMap.put(id,Math.min(range, rangeMap.get(id)));
                });
            }
        }
        System.out.println("New Range Map");
        rangeMap.forEach((id, r) -> {
            System.out.println(id + " : " + r);
        });
        return rangeMap;
    }

    public static List<String> readFileReverse(String filePath) {
        // List to store lines
        List<String> lines = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(filePath))) {
            // Read all lines into a list
            while (scanner.hasNextLine()) {
                lines.add(scanner.nextLine());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        return lines;
    }

}
