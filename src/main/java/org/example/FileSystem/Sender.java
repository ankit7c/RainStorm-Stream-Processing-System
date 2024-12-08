package org.example.FileSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Stream.Leader;
import org.example.Stream.Tuple;
import org.example.entities.*;

import java.io.*;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.example.entities.FileTransferManager.compareLogs;
import static org.example.entities.FileTransferManager.getFileOperations;

public class Sender {
    private ObjectMapper objectMapper;

    public Sender() {
        objectMapper = new ObjectMapper();
    }

    public String sendMessage(String IpAddress, int port, Message message) {
        try (Socket socket = new Socket(IpAddress, port+10);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String msg = objectMapper.writeValueAsString(message.getMessageContent());
            out.println(msg);
            return in.readLine();
        } catch (IOException e) {
//            e.printStackTrace();
            System.out.println(e.getMessage());
            return "Unsuccessful";
        }
    }


    public String getFileRequest(String IpAddress, int port, String localFileName, String hyDFSFileName, int fileReceiverPort) {
        try {
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "get_file");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("fileReceiverPort", String.valueOf(fileReceiverPort));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("localFileName", localFileName);
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("get_file",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    senderPort,
                    messageContent);
            return sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
            return "Unsuccessful";
        }
    }

    public Boolean getFileRequestHash(String IpAddress, int port, String cacheFileName, String hyDFSFileName) {
        try {
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "get_file_hash");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("get_file_hash",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    senderPort,
                    messageContent);
            String response = sendMessage(IpAddress, port, msg);
            System.out.println(response);
            if(response.contains("Unsuccessful")){
                return false;
            }else {
                FileChannel fileChannel = FileChannel.open(Paths.get("local/" + cacheFileName), StandardOpenOption.READ);
                String calculatedHash = FileData.calculateHash(fileChannel);
                return calculatedHash.equals(response);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public void mergeFile(String hyDFSFileName) {
        try {
            int fileNameHash = HashFunction.hash(hyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            String IpAddress = member.getIpAddress();
            String port = member.getPort();
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "merge");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("merge",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    senderPort,
                    messageContent);
            String response = sendMessage(IpAddress, Integer.parseInt(port), msg);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //send request to receive a file or send a request to upload a file
    public String uploadFile(String localFileName, String hyDFSFileName) throws IOException {
        //check if file present in local
        try {
            int fileNameHash = HashFunction.hash(hyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            FileTransferManager.getRequestQueue().addRequest(new FileSender(
                    localFileName,
                    hyDFSFileName,
                    member.getIpAddress(),
                    Integer.parseInt(member.getPort()),
                    "UPLOAD",
                    "CREATE",
                    ""));
        }catch (Exception e){
            e.printStackTrace();
            return "Unable to upload file";
        }
        return "File uploaded successfully";
    }

    public String get_File(String localFileName, String hyDFSFileName) {
        try {
            //If any node is not there then ask for next node
            //If file not present then return File not found, for that need to check in all replicas
            //This request should go by default to Co-ordinator and then it would return the server name with the filepath and then request the file.
            boolean isAbsent = true;
            int fileNameHash = HashFunction.hash(hyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            String IpAddress = member.getIpAddress();
            String port = member.getPort();
            if(LRUFileCache.FILE_CACHE.isFileInCache(hyDFSFileName)){
                if(LRUFileCache.FILE_CACHE.isFileOlder(hyDFSFileName)){
                    //check if file has changed with the owner
                    if(getFileRequestHash(IpAddress, Integer.parseInt(port), localFileName, hyDFSFileName)){
                        System.out.println("The file is already cached and saved at : local/" + hyDFSFileName);
                        isAbsent = false;
                    }
                }else{
                    System.out.println("The file is already cached and saved at : local/" + hyDFSFileName);
                    isAbsent = false;
                }
            }

            if(isAbsent){
                int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
                String result = getFileRequest(IpAddress, Integer.parseInt(port), localFileName, hyDFSFileName, fileReceiverPort);
                System.out.println("File receive was " + result);
            }
            return "Success";
        }catch (Exception e){
            e.printStackTrace();
            return "Unable to send receive file request";
        }
    }

    //send a request to append a file
    public void append_File(String localFileName, String hyDFSFileName){
        try {
            int fileNameHash = HashFunction.hash(hyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            FileTransferManager.getRequestQueue().addRequest(new FileSender(
                    localFileName,
                    hyDFSFileName,
                    member.getIpAddress(),
                    Integer.parseInt(member.getPort()),
                    "UPLOAD",
                    "APPEND",
                    ""));
        } catch (RuntimeException e) {
            System.out.println("File Append was unsuccessful");
        }
    }

    //get a replica from a node
    public String getFileFromReplica(String VMName, String localFileName, String hyDFSFileName) {
        try{
            int hash = HashFunction.hash(VMName);
            Member member = MembershipList.getMemberById(hash);
            String IpAddress = member.getIpAddress();
            String port = member.getPort();
            int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
            String result = getFileRequest(IpAddress, Integer.parseInt(port), localFileName, hyDFSFileName, fileReceiverPort);
            if (result.equals("Successful")) {
                System.out.println(result);
            }
            return result;
        }catch (Exception e){
            e.printStackTrace();
            return "Unable to receive file";
        }
    }

    //write a code to send multi appends.
    public void sendMultiAppendRequests(String hyDFSFileName, List<String> VMs, List<String> localFileNames) {
        for(int i=0; i<localFileNames.size(); i++) {
            int VMHash = HashFunction.hash(VMs.get(i));
            Member member = MembershipList.getMemberById(VMHash);
            try {
                Map<String, Object> messageContent = new HashMap<>();
                messageContent.put("messageName", "multi_append");
                messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
                messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
                messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
                messageContent.put("msgId", FDProperties.generateRandomMessageId());
                messageContent.put("localFileName", localFileNames.get(i));
                messageContent.put("hyDFSFileName", hyDFSFileName);
                String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
                Message msg = new Message("multi_append",
                        String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                        senderPort,
                        messageContent);
                String response = sendMessage(member.getIpAddress(), Integer.parseInt(member.getPort()), msg);
                System.out.println(response);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public HashMap<String, List<String>> getFileDetails(Member member, String request, String hyDFSFileNames) throws Exception {
        try {
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", request);
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            //hyDFS Filenames should be comma separated
            messageContent.put("hyDFSFileNames", hyDFSFileNames);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message(request,
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    senderPort,
                    messageContent);
            String response = sendMessage(member.getIpAddress(), Integer.parseInt(member.getPort()), msg);
            System.out.println(response);
            ObjectMapper objectMapper = new ObjectMapper();
            HashMap<String, List<String>> map = objectMapper.readValue(response, HashMap.class);
            return map;
        }catch (Exception e){
            throw new Exception("Could not able to reconnect");
        }
    }

    // Function is for merge and Re replication after failure
    public void updateReplicas(List<String> fileNames){
        //get two successors and ask them their fileOperation list to compare with ours for merging
        try {
            String files = "";
            for (String fileName : fileNames) {
                files += fileName + ",";
            }
            files = files.substring(0, files.length() - 1);
            List<Member> members = MembershipList.getNextMembers(MembershipList.selfId);
            for (Member member : members) {
                HashMap<String, List<String>> map = getFileDetails(member, "get_file_details", files);

                //Compare file details and take appropriate action
                for (String fileName : fileNames) {
                    if (map.containsKey(fileName) || map.get(fileName)!=null) {
                        // compare the list of events to check if files are consistent
                        // if not consistent then ask to replace the file
                        if(compareLogs(map.get(fileName), getFileOperations(fileName))){ // call the compare list function
                            FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                    "HyDFS/" + fileName,
                                    fileName,
                                    member.getIpAddress(),
                                    Integer.parseInt(member.getPort()),
                                    "REPLICA",
                                    "CREATE",
                                    ""));
                        }
                        // if yes then do nothing
                    } else {
                        //Send the file to the node.
                        FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                "HyDFS/" + fileName,
                                fileName,
                                member.getIpAddress(),
                                Integer.parseInt(member.getPort()),
                                "REPLICA",
                                "CREATE",
                                ""));
                    }
                    FileTransferManager.logEvent("File Operation : Upload : Successful : " + fileName);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        //If file not present then ask them to replicate the file. Can also use UPDATE/MERGE
        //If logs are inconsistent then it means appends may be in wrong order or not received any update
        //Send them those files under UPDATE/MERGE TYPE also with the file operations list.
        //whenever you do these operations put a log event of merge successful
    }

    public Map<String, Object>  setMessage(String messageName){
        Map<String, Object> messageContent = new HashMap<>();
        messageContent.put("messageName", messageName);
        messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
        messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
        messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
        messageContent.put("senderId", MembershipList.selfId);
        messageContent.put("msgId", FDProperties.generateRandomMessageId());
        return messageContent;
    }

    // Use below function when a source performing node fails
    public Map<Leader.WorkerTasks, String> setSource(Member member, List<Member> op1, String filename, String range) {
        Map<Leader.WorkerTasks, String> status = new HashMap<>();
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("set_source");
            messageContent.put("filename", filename);
            messageContent.put("range", range);
            messageContent.put("num_tasks", op1.size());
            int i=0;
            for (Member value : op1) {
                messageContent.put("op1_" + i, value.getId());
                i++;
            }
            Message msg = new Message("set_source",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            System.out.println("port:" + port);
            String[] resp = sendMessage(IpAddress, port, msg).split(",");
            status.put(new Leader.WorkerTasks("source", member, Integer.parseInt(resp[0]), Integer.parseInt(resp[1])), resp[0]);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    // Use below function when a split or OP1 performing node fails
    public Map<Leader.WorkerTasks, String> setOp1(Member member, List<Member> source, List<Member> op2, String op1_name, String pattern) {
        Map<Leader.WorkerTasks, String> status = new HashMap<>();
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("set_op1");
            messageContent.put("num_tasks", op2.size());
            messageContent.put("operation_name", op1_name);
            messageContent.put("pattern", pattern);
            int i=0;
            for (Member value : source) {
                messageContent.put("source_" + i, value.getId());
                i++;
            }
            i=0;
            for (Member value : op2) {
                System.out.println("value.getId() : " + value.getId());
                messageContent.put("op2_" + i, value.getId());
                i++;
            }
            Message msg = new Message("set_op1",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            String[] resp = sendMessage(IpAddress, port, msg).split(",");
            status.put(new Leader.WorkerTasks("op1", member, Integer.parseInt(resp[0]), Integer.parseInt(resp[1])), resp[0]);
        }
            catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    public Map<Leader.WorkerTasks, String> setFailedOp1(Member member, List<Member> source, List<Member> op2, String op1_name, String pattern, int failedWorkerID) {
        Map<Leader.WorkerTasks, String> status = new HashMap<>();
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("set_failed_op1");
            messageContent.put("num_tasks", op2.size());
            messageContent.put("operation_name", op1_name);
            messageContent.put("pattern", pattern);
            messageContent.put("failed_worker_ID", failedWorkerID);
            int i=0;
            for (Member value : source) {
                messageContent.put("source_" + i, value.getId());
                i++;
            }
            i=0;
            for (Member value : op2) {
                System.out.println("value.getId() : " + value.getId());
                messageContent.put("op2_" + i, value.getId());
                i++;
            }
            Message msg = new Message("set_failed_op1",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            String[] resp = sendMessage(IpAddress, port, msg).split(",");
            status.put(new Leader.WorkerTasks("op1", member, Integer.parseInt(resp[0]), Integer.parseInt(resp[1])), resp[0]);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    // Use below function when a count or OP2 performing node fails
    public Map<Leader.WorkerTasks, String> setOp2(Member member, List<Member> op1, String destFilename, String op2_name, String pattern) {
        Map<Leader.WorkerTasks, String> status = new HashMap<>();
        try {
            System.out.println("op2");
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("set_op2");
            messageContent.put("destFilename", destFilename);
            messageContent.put("num_tasks", op1.size());
            messageContent.put("operation_name", op2_name);
            messageContent.put("pattern", pattern);
            int i=0;
            for (Member value : op1) {
                messageContent.put("op1_" + i, value.getId());
                i++;
            }
            Message msg = new Message("set_op2",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            String[] resp = sendMessage(IpAddress, port, msg).split(",");
            status.put(new Leader.WorkerTasks("op2", member, Integer.parseInt(resp[0]), Integer.parseInt(resp[1])), resp[0]);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    public Map<Leader.WorkerTasks, String> setFailedOp2(Member member, List<Member> op1, String destFilename, String op2_name, String pattern, int failedWorkerID) {
        Map<Leader.WorkerTasks, String> status = new HashMap<>();
        try {
            System.out.println("op2");
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("set_failed_op2");
            messageContent.put("destFilename", destFilename);
            messageContent.put("num_tasks", op1.size());
            messageContent.put("operation_name", op2_name);
            messageContent.put("pattern", pattern);
            messageContent.put("failed_worker_ID", failedWorkerID);
            int i=0;
            for (Member value : op1) {
                messageContent.put("op1_" + i, value.getId());
                i++;
            }
            Message msg = new Message("set_failed_op2",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            String[] resp = sendMessage(IpAddress, port, msg).split(",");
            status.put(new Leader.WorkerTasks("op2", member, Integer.parseInt(resp[0]), Integer.parseInt(resp[1])), resp[0]);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    public Map<Leader.WorkerTasks, String> setRoles(List<Member> sources, List<Member> op1,
                                        List<Member> op2, String filename, List<String> ranges,
                                        String destFilename, String[] ops, String pattern1, String pattern2){
        Map<Leader.WorkerTasks, String> status = new HashMap<>();
        System.out.println("Pattern : " + pattern1 + pattern2);
        try{
            for(int i = 0; i<sources.size(); i++){
                Member member = sources.get(i);
                status.putAll(setSource(member, op1, filename, ranges.get(i)));
            }
            for (Member member : op1) {
                status.putAll(setOp1(member, sources, op2, ops[0], pattern1));
            }
            for (Member member : op2) {
                status.putAll(setOp2(member, op1, destFilename, ops[1], pattern2));
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public void startProcessing(Member member, int workerId, ArrayList<String> receiverPorts){
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("start_processing");
            messageContent.put("worker_id", workerId);
            messageContent.put("total_receiver_ports", receiverPorts.size());
            int i=0;
            for(String receiverPort : receiverPorts){
                messageContent.put("receiver_port_" + i, receiverPort);
                i++;
            }
            Message msg = new Message("start_processing",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void resendLines(Member member, int workerId, int lineNum){
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("resend_lines");
            messageContent.put("worker_id", workerId);
            messageContent.put("line_num", lineNum);
            Message msg = new Message("resend_lines",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendNewNodeData(Member member, int workerId, int failedWorkerId, String data, int newMemberId){
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("send_new_node_data");
            messageContent.put("worker_id", workerId);
            messageContent.put("data", data);
            messageContent.put("failed_worker_ID", failedWorkerId);
            messageContent.put("new_member_ID", newMemberId);
            Message msg = new Message("send_new_node_data",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public String sendTuple(Tuple tuple, String type, Member member, int receiverWorkerId, int senderWorkerId){
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("send_tuple");
            messageContent.put("receiver_worker_id", receiverWorkerId);
            messageContent.put("sender_worker_id", senderWorkerId);
            messageContent.put("type", type);
            messageContent.put("tuple_id", tuple.getId());
            messageContent.put("tuple_key", String.valueOf(tuple.getKey()));
            messageContent.put("tuple_value", String.valueOf(tuple.getValue()));
            Message msg = new Message("send_tuple",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            return sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
        return "Unsuccessful";
    }

    public void sendAckToParent(String tupleId, Member member, int receiverWorkerId, int senderWorkerId) throws Exception {
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            Map<String, Object> messageContent = setMessage("send_tuple_ack");
            messageContent.put("receiver_worker_id", receiverWorkerId);
            messageContent.put("sender_worker_id", senderWorkerId);
            messageContent.put("tuple_id", tupleId);
            Message msg = new Message("send_tuple_ack",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendTupleToLeader(String result, Member leader, int senderWorkerId){
        try {
            String IpAddress = leader.getIpAddress();
            int port = Integer.parseInt(leader.getPort());
            Map<String, Object> messageContent = setMessage("send_tuple_leader");
            messageContent.put("sender_worker_id", senderWorkerId);
            messageContent.put("result", result);
            Message msg = new Message("send_tuple_leader",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendEOFToLeader(Member leader, int senderWorkerId){
        try {
            String IpAddress = leader.getIpAddress();
            int port = Integer.parseInt(leader.getPort());
            Map<String, Object> messageContent = setMessage("send_eof_leader");
            messageContent.put("sender_worker_id", senderWorkerId);
            Message msg = new Message("send_eof_leader",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void killWorkers(Member member, int workerId){
        try {
            String IpAddress = member.getIpAddress();
            int port = Integer.parseInt(member.getPort());
            System.out.println("Member Name :" + member.getName() + ":" + workerId);
            Map<String, Object> messageContent = setMessage("kill_workers");
            messageContent.put("worker_id", workerId);
            Message msg = new Message("kill_workers",
                    String.valueOf(FDProperties.getFDProperties().get("machineIp")),
                    String.valueOf(FDProperties.getFDProperties().get("machinePort")),
                    messageContent);
            sendMessage(IpAddress, port, msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
