package org.example.Stream;

import org.example.FileSystem.HelperFunctions;
import org.example.FileSystem.Sender;
import org.example.entities.FDProperties;
import org.example.entities.Member;
import org.example.entities.MembershipList;
import org.example.entities.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Leader {
    public static class WorkerTasks {
        String type;
        Member member;
        Integer receiverPort;
        public WorkerTasks(String type, Member member, Integer receiverPort) {
            this.type = type;
            this.member = member;
            this.receiverPort = receiverPort;
        }
    }
    List<Member> sources = new ArrayList<>();
    List<Member> op1 = new ArrayList<Member>();
    List<Member> op2 = new ArrayList<Member>();
    List<String> ranges = new ArrayList<>();
    ConcurrentSkipListMap<Integer, WorkerTasks> workerIds = new ConcurrentSkipListMap<>();
    int pointer = 0;
    Sender sender = new Sender();
    String currFilename;
    String currDestFilename;

    //Add functions to send  control commands to worker nodes

    //Function to determine which nodes are active and assign tasks to each of them
    public void initializeNodes(String filename,
                                String destFilename, int num_tasks){
        ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
        //Remove itself
        memberslist.remove(MembershipList.selfId);
        int size = memberslist.size();
//        Map<Leader.WorkerTasks, String> result = new HashMap<>();
        try {
            long line = HelperFunctions.countLines(filename);
            long start = 0;
            for(int i = 0; i < num_tasks; i++){
                String range = ((i == num_tasks-1) ? (start + "," + line) : (start + "," + line / num_tasks));
                ranges.add(range);
                sources.add(memberslist.get(pointer %size));
//                status.putAll(sender.setSource(memberslist.get(pointer %size), op1, filename, ranges.get(i)));
                pointer++;
            }
            for(int i = 0; i < num_tasks; i++){
                op1.add(memberslist.get(pointer %size));
//                status.putAll(sender.setOp1(memberslist.get(pointer%size), sources, op2));
                pointer++;
            }
            for(int i = 0; i < num_tasks; i++){
                op2.add(memberslist.get(pointer%size));
//                status.putAll(sender.setOp2(memberslist.get(pointer%size), op1, destFilename));
                pointer++;
            }
            //Call each node and assign the role
            Map<Leader.WorkerTasks, String> result = sender.setRoles(sources, op1, op2, filename, ranges, destFilename);
            //Check if each result is pass, if not then pick new node and ask it to handle the task
            result.forEach(((workerTasks, s) -> {
                if(Integer.parseInt(s) == -1){
                    updateFailedNode(workerTasks.member);
                }
                workerIds.put(Integer.valueOf(s),workerTasks);
            }));

            //Send the Receiver ports to all workers
//            Map<Integer, String> receiverPorts = new HashMap<>();
            ArrayList<String> receiverPorts = new ArrayList<>();
            workerIds.forEach((workerId,workerTask) -> {
                //Along with worker id send the data
//                receiverPorts.put(workerTask.member.getId(), String.valueOf(workerTask.receiverPort));
                receiverPorts.add(workerTask.member.getId() + "," + workerTask.receiverPort);
            });
            workerIds.forEach((workerId,workerTask) -> {
                Member member = memberslist.get(workerTask.member.getId());
                sender.startProcessing(member, workerId, receiverPorts);
            });

            pointer = pointer%size;
            this.currFilename = filename;
            this.currDestFilename = destFilename;

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //TODO Function to take action when a node is failed
    public void updateFailedNode(Member failMember){
        String name = failMember.getName();
        String type = "";
        for(Member member : sources){
            if(member.getName().equals(name)){
                type = "source";
                break;
            }
        }
        for(Member member : op1){
            if(member.getName().equals(name)){
                type = "op1";
                break;
            }
        }
        for(Member member : op2){
            if(member.getName().equals(name)){
                type = "op2";
                break;
            }
        }
        //Get the next free node from the list
        ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
        //Remove itself
        memberslist.remove(MembershipList.selfId);
        pointer = memberslist.get(failMember.getId()).getId();
        pointer++;
        Member member = memberslist.get(pointer%memberslist.size());
        //TODO take appropriate action based on type of failed node
        switch (type){
            case "source":
                //TODO check source nodes on logs to see where it failed
                String range = "";
                //TODO give the new node the lines and addresses of next nodes to send data
                sender.setSource(member, op1, currFilename, range);
                break;
            case "op1":
                //TODO same as above see the logs to determine which ack was sent last and
                //TODO when they join the system the  previous nodes should track the acks they sent and sent from appropriate location
                // we will also need to check the HyDFS logs to see the acks processed.
                //TODO Send a node a message that its next node has failed and it needs to change its next machine.
                sender.setOp1(member, sources, op2);
               break;
            case "op2":
                //TODO check Count node logs and hydfs data file to see where it failed and ask split to play from that specific point
                sender.setOp2(member, op1, currDestFilename);
               break;
            default:
                System.out.println("Invalid type, Not able to found the failed node in the current working nodes list");
                break;
        }

    }


}
