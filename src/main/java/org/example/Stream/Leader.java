package org.example.Stream;

import org.example.FileSystem.HelperFunctions;
import org.example.FileSystem.Sender;
import org.example.entities.Member;
import org.example.entities.MembershipList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Leader {
    List<Member> sources = new ArrayList<Member>();
    List<Member> op1 = new ArrayList<Member>();
    List<Member> op2 = new ArrayList<Member>();
    List<String> ranges = new ArrayList<>();
    int pointer = 0;
    Sender sender = new Sender();
    String currFilename;
    String currDestFilename;
    //TODO Add functions to send  control commands to worker nodes

    //TODO Function to determine which nodes are active and assign tasks to each of them
    public void initializeNodes(String filename,
                                String destFilename, int num_tasks){
        ConcurrentSkipListMap<Integer, Member> memberslist = MembershipList.memberslist;
        //Remove itself
        memberslist.remove(MembershipList.selfId);
        int size = memberslist.size();
        try {
            long line = HelperFunctions.countLines(filename);
            long start = 0;
            for(int i = 0; i < num_tasks; i++){
                String range = ((i == num_tasks-1) ? (start + "," + line) : (start + "," + line / num_tasks));
                ranges.add(range);
                sources.add(memberslist.get(pointer %size));
                pointer++;
            }
            for(int i = 0; i < num_tasks; i++){
                op1.add(memberslist.get(pointer %size));
                pointer++;
            }
            for(int i = 0; i < num_tasks; i++){
                op2.add(memberslist.get(pointer%size));
                pointer++;
            }
            //Call each node and assign the role
            Map<Member, String> result = sender.setRoles(sources, op1, op2, filename, ranges, destFilename);
            //TODO check if each result is pass, if not then pick new node and ask it to handle the task
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
                //TODO check source nodes logs to see where it failed
                String range = "";
                //TODO give the new node the lines and addresses of next nodes to send data
                sender.setSource(member, op1, currFilename, range);
                break;
            case "op1":
                //TODO when they join the system the  previous nodes should track the acks they sent and sent from appropriate location
                // we will also need to check the HyDFS logs to see the acks processed.
                //TODO Send a node a message that its next node has failed and it needs to change its next machine.
                sender.setOp1(member, sources, op2);
               break;
            case "op2":
                sender.setOp2(member, op1, currDestFilename);
               break;
            default:
                System.out.println("Invalid type, Not able to found the failed node in the current working nodes list");
                break;
        }

    }


}
