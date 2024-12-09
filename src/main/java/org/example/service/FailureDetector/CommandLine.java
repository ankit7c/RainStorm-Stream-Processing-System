package org.example.service.FailureDetector;

import org.example.Client;
import org.example.FileSystem.HashFunction;
import org.example.FileSystem.HelperFunctions;
import org.example.FileSystem.Sender;
import org.example.Server;
import org.example.Stream.Leader;
import org.example.entities.FDProperties;
import org.example.entities.FileData;
import org.example.entities.Member;
import org.example.entities.MembershipList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.entities.FDProperties.fDProperties;

// Comma class that will be executed by multiple threads

/**
 * This Class is handle command line commands
 */
public class CommandLine implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CommandLine.class);
    private ConcurrentHashMap<String, Integer> map;
    private String threadName;

    @Override
    public void run() {
        // Each thread updates the map with its own name as the key
        while(true) {
            System.out.println("Enter The command");
            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            String[] list = command.split(" ");
            Dissemination d = new Dissemination();
            Sender sender = new Sender();
            try {
                if (command.startsWith("grep")) {
                    Client c = new Client();
                    c.runClient(command);
                } else if(command.startsWith("drop")) {
                    double dropProb = Double.parseDouble(command.substring(4));
                    fDProperties.put("dropProbability", dropProb);
                } else {
                    switch (list[0]) {
                        case "list_mem":
                            System.out.println("Membership List");
                            MembershipList.printMembersId();
                            break;

                        case "list_self":
                            System.out.println("Node self id");
                            String id = FDProperties.getFDProperties().get("machineIp") + "_" + String.valueOf(FDProperties.getFDProperties().get("machinePort")) + "_"
                                    + FDProperties.getFDProperties().get("versionNo");
                            System.out.println(id);
                            break;
                        case "join":
                            System.out.println("Joining Node");
                            Server s = new Server();
                            s.startServer();
                            break;
                        case "leave":
                            d.sendLeaveMessage();
                            break;

                        case "enable_sus":

                        case "disable_sus":
                            d.sendSwitch();
                            break;

                        case "status_sus":
                            System.out.println(FDProperties.getFDProperties().get("isSuspicionModeOn"));
                            break;

                        // Commands for Distributed File System Handling
                        case "create":
                            sender.uploadFile(list[1], list[2]);
                            break;
                        case "get":
                            sender.get_File(list[1], list[2]);
                            break;
                        case "append":
                            sender.append_File(list[1], list[2]);
                            break;
                        case "merge":
                            String hyDFSFile = list[1];
                            sender.mergeFile(hyDFSFile);
                            break;
                        case "ls":
                            int fileNameHash = HashFunction.hash(list[1]);
                            List<Member> memberslist = new ArrayList<>();
                            memberslist.add(MembershipList.getMemberById(fileNameHash));
                            memberslist.addAll(MembershipList.getNextMembers(fileNameHash));
                            System.out.println("File " + list[1] + " with id " + fileNameHash + " is present at below machines");
                            for (Member member : memberslist) {
                                System.out.println("Member id: " + member.getId());
                            }
                            break;
                        case "store":
//                            list the set of file names (along with their IDs) that are replicated (stored) on HyDFS at this
//                            (local) process/VM. This should NOT include files stored on the local file system.
//                            Also, print the process/VM’s ID on the ring.
                            System.out.println("Owned HyDFS files on the " +
                                    String.valueOf(FDProperties.getFDProperties().get("machineName")) +
                                    " with ring id " + MembershipList.selfId);
                            for(String fileName : FileData.getOwnedFiles()) {
                                System.out.println(fileName);
                            }
                            System.out.println("Replicated HyDFS files on the " +
                                    String.valueOf(FDProperties.getFDProperties().get("machineName")) +
                                    " with ring id " + MembershipList.selfId);
                            FileData.getReplicaMap().forEach((k,v)->{
                                System.out.println(k + " " + v);
                            });
                            break;
                        case "getFromReplica":
                            sender.getFileFromReplica(list[1], list[2], list[3]);
                            break;
                        case "list_mem_ids":
//                            augment list_mem from MP2 to also print the ID on the ring which each node in the membership list maps to
                            MembershipList.memberslist.forEach((k, v) -> System.out.println(k + "," + v.getName()));
                            break;
                        case "multiappend":
                            String hyDFSFileName = list[1];
                            List<String> VMs = new ArrayList<>();
                            List<String> localFiles = new ArrayList<>();
                            for(int i=2; i<list.length; i++) {
                                if(list[i].contains("Machine")){
                                    VMs.add(list[i]);
                                }else{
                                    localFiles.add(list[i]);
                                }
                            }
                            sender.sendMultiAppendRequests(hyDFSFileName, VMs, localFiles);
                            break;
                        case "list_self_id":
                            System.out.println("Self ID: " + MembershipList.selfId);
                            break;

                        case "merge_test4kb":
                            long startTime1 = System.currentTimeMillis();
                            String hyDFsFileName1 = "base_file_10MB.txt";
                            int concurrentClients = Integer.parseInt(list[1]);
                            int size1 = Integer.parseInt(list[2]);
                            List<String> vmNames = new ArrayList<>();
                            HelperFunctions.listFiller(concurrentClients, vmNames, size1);
                            List<String> localFilesNames = new ArrayList<>();
                            for(int i=0;i<size1;i++){
                                localFilesNames.add("input/append_data_4KB.txt");
                            }
                            long endTime1 = System.currentTimeMillis();
                            long duration1 = endTime1 - startTime1;
                            System.out.println("Start time was " + startTime1 + " end time is "+ endTime1 +"time taken "+duration1);
                            sender.sendMultiAppendRequests(hyDFsFileName1, vmNames, localFilesNames);
                            break;

                        case "merge_test40kb":
                            long startTime2 = System.currentTimeMillis();
                            String hyDFsFileName2 = "base_file_10MB.txt";
                            int concurrentClients2 = Integer.parseInt(list[1]);
                            int size2 = Integer.parseInt(list[2]);
                            List<String> vmNames2 = new ArrayList<>();
                            HelperFunctions.listFiller(concurrentClients2, vmNames2, size2);
                            List<String> localFilesNames2 = new ArrayList<>();
                            for(int i=0;i<size2;i++){
                                localFilesNames2.add("input/append_data_40KB.txt");
                            }
                            sender.sendMultiAppendRequests(hyDFsFileName2, vmNames2, localFilesNames2);
                            long endTime2 = System.currentTimeMillis();
                            long duration2 = endTime2 - startTime2;
                            System.out.println("Start time was " + startTime2 + " end time is "+ endTime2 +"time taken "+duration2);
                            break;

                        // Commands for Rainstorm
                        // rainstorm Filter ExtractColumns input/TrafficSigns_10000.csv hydfs_dest_filename.txt 1 "Streetname" "Streetname"
                        // rainstorm FilterOnColumn Count input/TrafficSigns_1000.csv countCat.txt 1 "Punched_Telespar" "Punched_Telespar"
                        // rainstorm FilterOnColumn Count input/TrafficSigns_1000.csv countCat.txt 1 "Power_Pole" "Power_Pole"
                        // rainstorm Filter ExtractColumns input/Traffic_Signs.csv hydfs_dest_filename.txt 1
                        case "rainstorm":
                            String[] ops = Arrays.copyOfRange(list, 1, 3);
                            String filename = list[3];
                            String dest_filename = list[4];
                            String num_tasks = list[5];
                            String pattern1 = list[6].replace("\"", "");
                            pattern1 = pattern1.replace("_", " ");
                            String pattern2 = list[7].replace("\"", "");
                            pattern2 = pattern2.replace("_", " ");
                            System.out.println(pattern2 + pattern1);
                            Leader.initializeNodes(filename,dest_filename, Integer.parseInt(num_tasks), ops, pattern1, pattern2);
                            break;

                        default:
                            System.out.println("Invalid command");
                            logger.error("Invalid command");

                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error in Commandline while exectuing  command {}  Error  : {}", command, e);
            }
        }
    }
}
