package org.example.Stream;

import org.example.Executor.Test;
import org.example.FileSystem.HelperFunctions;
import org.example.FileSystem.Sender;
import org.example.entities.Member;
import org.example.entities.MembershipList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    static List<Member> sources = new ArrayList<>();
    static List<Member> op1 = new ArrayList<>();
    static List<Member> op2 = new ArrayList<>();
    static List<String> ranges = new ArrayList<>();
    static ConcurrentSkipListMap<Integer, WorkerTasks> workerIds = new ConcurrentSkipListMap<>();
    static int pointer = 0;
    static Sender sender = new Sender();
    static String currFilename;
    static String currDestFilename;
    static String opertion1Name;
    static String opertion2Name;
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
        int size = memberslist.size();
        try {
            long line = HelperFunctions.countLines(filename);
            long start = 0;
            for(int i = 0; i < num_tasks; i++){
                String range = ((i == num_tasks-1) ? (start + "," + line) : (start + "," + (start + (line / num_tasks))));
                start = start + (line / num_tasks);
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
//                if(Integer.parseInt(s) == -1){
//                    updateFailedNode(workerTasks.member);
//                }
                System.out.println("Adding members :" + Integer.valueOf(s));
                workerIds.put(Integer.valueOf(s),workerTasks);
            }));

            //Send the Receiver ports to all workers
            ArrayList<String> receiverPorts = new ArrayList<>();
            workerIds.forEach((workerId,workerTask) -> {
                //Along with worker id send the data
                receiverPorts.add(workerTask.type + ":" + workerTask.member.getId() + ":" + workerTask.workerId + ":" + workerTask.receiverPort);
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
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        //TODO code to clear all the rainstorm data
        System.out.println("Clearing all caches");
        sources.clear();
        op1.clear();
        op2.clear();
        ranges.clear();
        workerIds.clear();
        pointer = 0;
        totalEOFs.clear();
    }

    //TODO Function to take action when a node is failed
//    public void updateFailedNode(Member failMember){
//        String name = failMember.getName();
//        //TODO get the failed members Worker Ids
//        ArrayList<WorkerTasks> Failedids = new ArrayList<>();
//        workerIds.forEach((workerId,workerTask) -> {
//            if(workerTask.member.getId() == failMember.getId()){
//                Failedids.add(workerTask);
//            }
//        });
//
//        for(WorkerTasks workerTask : Failedids){
//            String type = workerTask.type;
//            //Get the next free node from the list
//            ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
//            //Remove itself
//            memberslist.remove(MembershipList.selfId);
//            ArrayList<Integer> ids = new ArrayList<>();
//            memberslist.forEach((k,v) -> ids.add(k));
//            int size = memberslist.size();
//            pointer++;
//            Member member = failMember;
//            //If we choose the failed Member again
//            while(member.getId() == failMember.getId()){
//                member = memberslist.get(pointer%memberslist.size());
//            }
//            //TODO take appropriate action based on type of failed node
//            switch (type){
//                case "source":
//                    //TODO check source nodes on logs to see where it failed
//                    String range = "";
//                    //TODO give the new node the lines and addresses of next nodes to send data
//                    sender.setSource(member, op1, currFilename, range);
//                    break;
//                case "op1":
//                    //TODO same as above see the logs to determine which ack was sent last and
//                    //TODO when they join the system the  previous nodes should track the acks they sent and sent from appropriate location
//                    // we will also need to check the HyDFS logs to see the acks processed.
//                    //TODO Send a node a message that its next node has failed and it needs to change its next machine.
//                    sender.setOp1(member, sources, op2, opertion1Name);
//                    break;
//                case "op2":
//                    //TODO check Count node logs and hydfs data file to see where it failed and ask split to play from that specific point
//                    sender.setOp2(member, op1, currDestFilename, opertion2Name);
//                    break;
//                default:
//                    System.out.println("Invalid type, Not able to found the failed node in the current working nodes list");
//                    break;
//            }
//        }
//
//
//    }

}
