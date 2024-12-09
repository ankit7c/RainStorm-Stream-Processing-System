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
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class Leader {
    public static class WorkerTasks {
        String type;
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

    //Add functions to send  control commands to worker nodes

    //Function to determine which nodes are active and assign tasks to each of them
    public static void initializeNodes(String filename,
                                String destFilename, int num_tasks, String[] ops, String pattern1, String pattern2) {
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

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void processEOFs(int workerId){
        totalEOFs.put(workerId,1);
        if(totalEOFs.size() == op2.size()){
            try {
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
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    //TODO Function to take action when a node is failed
    public static void updateFailedNode(int failMemberId){
        //Get the failed members Worker Ids
        Member failMember = null;
        ArrayList<Integer> Failedids = new ArrayList<>();
        for (Map.Entry<Integer, WorkerTasks> entry : workerIds.entrySet()) {
            Integer workerId = entry.getKey();
            WorkerTasks value = entry.getValue();
            if (value.member.getId() == failMemberId) {
                Failedids.add(workerId);
                failMember = value.member;
            }
        }
//        Failedid;
        for(Integer failedId : Failedids){
            WorkerTasks workerTask = workerIds.get(failedId);
            String type = workerTask.type;
            //Get the next free node from the list
            //TODO Don't include the failed member here. The failed member should be removed firs to start this process
            ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
            //Remove itself
            memberslist.remove(MembershipList.selfId);
            ArrayList<Integer> ids = new ArrayList<>();
            memberslist.forEach((k,v) -> ids.add(k));
            pointer++;
            Member member = failMember;
            //If we choose the failed Member again
            while(member.getId() == failMemberId){
                member = memberslist.get(pointer%memberslist.size());
                pointer++;
            }
            System.out.println("New Machine to replace " + failMemberId + " is " + member.getId());
            workerIds.remove(failedId);
            //TODO Get the log of the failed node
            //TODO determine the new ranges
            //TODO Ask the new node to set up the role
            //TODO If aggregator ask it to load ser + log
            switch (type){
                case "source":
                    break;
                case "op1": {
                    Map<WorkerTasks, String> result = sender.setFailedOp1(member, sources, op2, operation1Name, patternop1, workerTask.workerId);
                    result.forEach(((workerTasks, s) -> {
                        System.out.println("Adding members :" + Integer.valueOf(s));
                        workerIds.put(Integer.valueOf(s), workerTask);
                    }));

                    ArrayList<String> receiverPorts = new ArrayList<>();
                    workerIds.forEach((workerId, workerTasks) -> {
                        //Along with worker id send the data
                        receiverPorts.add(workerTasks.type + ":" + workerTasks.member.getId() + ":" + workerTasks.workerId + ":" + workerTasks.receiverPort);
                    });
                    sender.startProcessing(member, workerTask.workerId, receiverPorts);

                    String newWorkerIDData = workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort;
                    int newMemberId = member.getId();
                    workerIds.forEach(((workerId, workerTasks) -> {
                        Member tempmember = memberslist.get(workerTask.member.getId());
                        sender.sendNewNodeData(tempmember, workerId, failedId, newWorkerIDData, newMemberId);
                    }));
                }
                    break;
                case "op2": {
                    Map<WorkerTasks, String> result = sender.setFailedOp2(member, op1, currDestFilename, operation2Name, patternop2, workerTask.workerId);
                    result.forEach(((workerTasks, s) -> {
                        System.out.println("Adding members :" + Integer.valueOf(s));
                        workerIds.put(Integer.valueOf(s), workerTask);
                    }));

                    ArrayList<String> receiverPorts = new ArrayList<>();
                    workerIds.forEach((workerId, workerTasks) -> {
                        //Along with worker id send the data
                        receiverPorts.add(workerTasks.type + ":" + workerTasks.member.getId() + ":" + workerTasks.workerId + ":" + workerTasks.receiverPort);
                    });
                    sender.startProcessing(member, workerTask.workerId, receiverPorts);
                    //Ask the nodes to replace the failed node in their lists
                    //Ask source to resend the data based on ranges calculated earlier
                    String newWorkerIDData = workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort;
                    int newMemberId = member.getId();
                    workerIds.forEach(((workerId, workerTasks) -> {
                        Member tempmember = memberslist.get(workerTask.member.getId());
                        sender.sendNewNodeData(tempmember, workerId, failedId, newWorkerIDData, newMemberId);
                    }));
                }
                    break;
                default:
                    System.out.println("Invalid type, Not able to found the failed node in the current working nodes list");
                    break;
            }
            getLogs(failedId, type);
            HashMap<Integer ,Integer> rangeMap = getNewRanges(failedId, type);
            rangeMap.forEach((sid, lineNo)->{
                sender.resendLines(workerIds.get(sid).member, sid, lineNo);
            });
        }


    }

    public static void getLogs(int failedWorkerId, String type){
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
    }

    public static HashMap<Integer ,Integer> getNewRanges(int failedWorkerId, String type){
        String fileName = failedWorkerId + "_" + type +"_log.txt";
        HashMap<Integer ,Integer> rangeMap = new HashMap<>();
        List<String> lines = readFileReverse(fileName);
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
                            rangeMap.put(Integer.valueOf(senderId), Math.max(lineNo, rangeMap.get(senderId)));
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
