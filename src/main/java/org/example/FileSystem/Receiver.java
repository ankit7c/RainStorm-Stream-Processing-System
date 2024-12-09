package org.example.FileSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Stream.Leader;
import org.example.Stream.Tuple;
import org.example.Stream.Worker;
import org.example.Stream.WorkerManager;
import org.example.entities.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Receiver extends Thread {

    public void receiveMessage(){
        int serverPort = (int) FDProperties.getFDProperties().get("machinePort") + 10;
        try (var serverSocket = new ServerSocket(serverPort)) { // Server listening on port 5000
            System.out.println("Server is listening on port " + serverPort);

            // Accept client connection
            while (true) {
                try (Socket socket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

//                    System.out.println("Client connected");

                    // Read message from client
                    String received = in.readLine();
                    Message message = Message.process(socket.getInetAddress(), String.valueOf(socket.getPort()), received);
//                    System.out.println("Received from client: " + message);
                    String response = "Successful";
                    switch (message.getMessageName()){
                        case "get_file" :
                            //TODO write code for sending the file which is demanded
                            try {
                                String hyDFSFileName = String.valueOf(message.getMessageContent().get("hyDFSFileName"));
                                String localFileName = String.valueOf(message.getMessageContent().get("localFileName"));
                                if(FileData.checkFilePresent(hyDFSFileName)) {
                                    FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                            "HyDFS/" + hyDFSFileName,
                                            localFileName,
                                            message.getIpAddress().getHostAddress(),
                                            Integer.parseInt(String.valueOf(message.getMessageContent().get("senderPort"))),
                                            "GET",
                                            "CREATE",
                                            "Sending Requested File"
                                    ));
                                    response = "Successful";
                                }else {
                                    response = "Unsuccessful file not found";
                                }
                                out.println(response);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "get_log" :
                            //TODO write code for sending the file which is demanded
                            try {
                                String hyDFSFileName = String.valueOf(message.getMessageContent().get("hyDFSFileName"));
                                String localFileName = String.valueOf(message.getMessageContent().get("localFileName"));
                                if(FileData.checkFilePresent(hyDFSFileName)) {
                                    FileSender fileSender = new FileSender(
                                            "HyDFS/" + hyDFSFileName,
                                            localFileName,
                                            message.getIpAddress().getHostAddress(),
                                            Integer.parseInt(String.valueOf(message.getMessageContent().get("senderPort"))),
                                            "GET",
                                            "CREATE",
                                            "Sending Requested File"
                                    );
                                    fileSender.run();
                                    response = "Successful";
                                }else {
                                    response = "Unsuccessful file not found";
                                }
                                out.println(response);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "get_file_from_replica" :
                            try {
                                String hyDFSFileName = String.valueOf(message.getMessageContent().get("hyDFSFileName"));
                                String localFileName = String.valueOf(message.getMessageContent().get("localFileName"));
                                if(FileData.checkFilePresent(hyDFSFileName)){
                                    FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                            "HyDFS/" + hyDFSFileName,
                                            localFileName,
                                            message.getIpAddress().getHostAddress(),
                                            Integer.parseInt(String.valueOf(message.getMessageContent().get("senderPort"))),
                                            "GET",
                                            "CREATE",
                                            "Sending Requested File"
                                    ));
                                    response = "Successful";
                                }else {
                                    response = "Unsuccessful file not found";
                                }
                                out.println(response);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "get_file_hash":
                            try {
                                String hyDFSFileName = String.valueOf(message.getMessageContent().get("hyDFSFileName"));
                                if(FileData.checkFilePresent(hyDFSFileName)){
                                    FileChannel fileChannel = FileChannel.open(Paths.get("HyDFS/" + hyDFSFileName), StandardOpenOption.READ);
                                    response = FileData.calculateHash(fileChannel);
                                }else {
                                    response = "Unsuccessful file not found";
                                }
                                //System.out.println("Printing from 101"+response);
                                out.println(response);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "multi_append":
                            Sender sender = new Sender();
                            String localFileName = String.valueOf(message.getMessageContent().get("localFileName"));
                            String hyDFSFileName = String.valueOf(message.getMessageContent().get("hyDFSFileName"));
                            sender.append_File(localFileName, hyDFSFileName);
                            break;
                        case "merge":
                            try {
                                String FileName = String.valueOf(message.getMessageContent().get("hyDFSFileName"));
                                Sender s = new Sender();
                                s.updateReplicas(Arrays.asList(FileName));
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "get_file_details":
                            String[] requestFiles = String.valueOf(message.getMessageContent().get("hyDFSFileNames")).split(",");
                            HashMap<String, List<String>> map = new HashMap<>();
                            for(String requestFile : requestFiles){
                                if(FileData.checkFilePresent(requestFile)) {
                                    map.put(requestFile, FileTransferManager.getFileOperations(requestFile));
                                }
                            }
                            ObjectMapper objectMapper = new ObjectMapper();
                            out.println(objectMapper.writeValueAsString(map));
                            break;

                            // Add code below to handle failures
                        case "set_source":
                            //Action a source needs to take when a Leader asks to perform a task
                            try {
                                System.out.println("set_source");
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                WorkerManager.leader = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                List<Member> op1s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op1s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op1_" + i)))));
                                }
                                String range = String.valueOf(message.getMessageContent().get("range"));
                                String filename = String.valueOf(message.getMessageContent().get("filename"));
                                Worker worker = new Worker("source", null, op1s, null, range, filename, null, null, "");
                                int id = WorkerManager.initializeWorker(worker);
                                //Return the id to leader
                                out.println(id + "," + WorkerManager.workers.get(id).receiverPort);
                            }catch (Exception e){
                                out.println(-1);
                                e.printStackTrace();
                            }
                            break;
                        case "set_op1":
                            //Action a OP1 needs to take when a Leader asks to perform a task
                            try {
                                System.out.println("set_op1");
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                String opName = String.valueOf(message.getMessageContent().get("operation_name"));
                                String pattern = String.valueOf(message.getMessageContent().get("pattern"));
                                System.out.println("Pattern : " + pattern);
                                WorkerManager.leader = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                List<Member> sources = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    sources.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("source_" + i)))));
                                }
                                List<Member> op2s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op2s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op2_" + i)))));
                                }
                                Worker worker = new Worker("op1", sources, null, op2s, null, null, null, opName, pattern);
                                int id = WorkerManager.initializeWorker(worker);
                                //Return the id to leader
                                out.println(id + "," + WorkerManager.workers.get(id).receiverPort);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "set_failed_op1":
                            //Action a OP1 needs to take when a Leader asks to perform a task
                            try {
                                System.out.println("set_failed_op1");
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                int failedWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("failed_worker_ID")));
                                String opName = String.valueOf(message.getMessageContent().get("operation_name"));
                                String pattern = String.valueOf(message.getMessageContent().get("pattern"));
                                System.out.println("Pattern : " + pattern);
                                WorkerManager.leader = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                List<Member> sources = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    sources.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("source_" + i)))));
                                }
                                List<Member> op2s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op2s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op2_" + i)))));
                                }
                                Worker worker = new Worker("op1", sources, null, op2s, null, null, null, opName, pattern);
                                int id = WorkerManager.initializeWorker(worker);
                                WorkerManager.initializeFailedWorker(worker, failedWorkerId);
                                //Return the id to leader
                                out.println(id + "," + WorkerManager.workers.get(id).receiverPort);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "set_op2":
                            //Action a OP2 needs to take when a Leader asks to perform a task
                            try {
                                System.out.println("set_op2");
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                String opName = String.valueOf(message.getMessageContent().get("operation_name"));
                                String pattern = String.valueOf(message.getMessageContent().get("pattern"));
                                System.out.println("Pattern : " + pattern);
                                WorkerManager.leader = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                List<Member> op1s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op1s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op1_" + i)))));
                                }
                                String destFilename = String.valueOf(message.getMessageContent().get("destFilename"));
                                Worker worker = new Worker("op2", null, op1s, null, null, null, destFilename, opName, pattern);
                                int id = WorkerManager.initializeWorker(worker);
                                //Return the id to leader
                                out.println(id + "," + WorkerManager.workers.get(id).receiverPort);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "set_failed_op2":
                            //Action a OP2 needs to take when a Leader asks to perform a task
                            try {
//                                deleteHyDFSFiles.deleteLogs();
                                System.out.println("set_failed_op2");
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                int failedWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("failed_worker_ID")));
                                String opName = String.valueOf(message.getMessageContent().get("operation_name"));
                                String pattern = String.valueOf(message.getMessageContent().get("pattern"));
                                System.out.println("Pattern : " + pattern);
                                WorkerManager.leader = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                List<Member> op1s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op1s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op1_" + i)))));
                                }
                                String destFilename = String.valueOf(message.getMessageContent().get("destFilename"));
                                Worker worker = new Worker("op2", null, op1s, null, null, null, destFilename, opName, pattern);
                                int id = WorkerManager.initializeWorker(worker);
                                WorkerManager.initializeFailedWorker(worker, failedWorkerId);
                                //Return the id to leader
                                out.println(id + "," + WorkerManager.workers.get(id).receiverPort);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "start_processing":
                            //TODO based on worker id Manager should set the receiver ports for the specific worker to listen and send data
                            try {
                                int workerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("worker_id")));
                                int totReceiverPorts = Integer.parseInt(String.valueOf(message.getMessageContent().get("total_receiver_ports")));
                                ArrayList<String> receiverPorts = new ArrayList<>();
                                for (int i = 0; i < totReceiverPorts; i++) {
                                    receiverPorts.add(String.valueOf(message.getMessageContent().get("receiver_port_" + i)));
                                    System.out.println("Received String : " + String.valueOf(message.getMessageContent().get("receiver_port_" + i)));
                                }
                                WorkerManager.workers.get(workerId).setReceiverPorts(receiverPorts);
                                System.out.println("Starting Worker:" + workerId);
                                WorkerManager.startWorker(workerId);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "send_new_node_data":
                            //TODO based on worker id Manager should set the receiver ports for the specific worker to listen and send data
                            try {
                                int newWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("worker_id")));
                                int newWorkerMemberId = Integer.parseInt(String.valueOf(message.getMessageContent().get("new_member_ID")));
                                int failedWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("failed_worker_ID")));
                                String data = String.valueOf(message.getMessageContent().get("data"));
                                ArrayList<String> receiverPorts = new ArrayList<>();
                                //TODO set the new worker in the List
                                WorkerManager.workers.forEach((id, worker)->{
                                    worker.setNewWorker(newWorkerId, failedWorkerId, newWorkerMemberId);
                                });
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "send_tuple":
                            try {
                                int receiverWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("receiver_worker_id")));
                                int senderWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("sender_worker_id")));
                                System.out.println("At line no 214: " + message.getMessageContent().get("tuple_id"));
                                String tupleType = String.valueOf(message.getMessageContent().get("type"));
                                String tupleId = String.valueOf(message.getMessageContent().get("tuple_id"));
                                String tupleKey = String.valueOf(message.getMessageContent().get("tuple_key"));
                                String tupleValue = String.valueOf(message.getMessageContent().get("tuple_value"));
                                Tuple tuple = new Tuple(tupleId, tupleKey, tupleValue);
                                Member member = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                WorkerManager.assignTuple(receiverWorkerId, senderWorkerId, member, tuple, tupleType);
                                out.println("Successful");
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "send_tuple_ack":
                            try {
                                int receiverWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("receiver_worker_id")));
                                int senderWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("sender_worker_id")));
                                Member member = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                String tupleId = String.valueOf(message.getMessageContent().get("tuple_id"));
                                WorkerManager.assignAck(receiverWorkerId, senderWorkerId, member, tupleId);
                                out.println("Successful");
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "send_tuple_leader":
                            try {
                                int senderWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("sender_worker_id")));
                                String result = String.valueOf(message.getMessageContent().get("result"));
                                Member member = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                System.out.println(member.getName() + " : " + senderWorkerId + " : " + result);
                                out.println("Successful");
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "send_eof_leader":
                            try {
                                int senderWorkerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("sender_worker_id")));
                                Member member = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                Leader.processEOFs(senderWorkerId);
                                out.println("Successful");
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "kill_workers":
                            try {
                                int workerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("worker_id")));
                                Member member = MembershipList.getMemberById(Integer.parseInt(String.valueOf(message.getMessageContent().get("senderId"))));
                                WorkerManager.killWorker(workerId);
                                out.println("Successful");
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "resend_lines":
                            try {
                                int workerId = Integer.parseInt(String.valueOf(message.getMessageContent().get("worker_id")));
                                int lineNo = Integer.parseInt(String.valueOf(message.getMessageContent().get("line_num")));
                                WorkerManager.restartSource(workerId, lineNo);
                                out.println("Successful");
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                    }
                    // Send response back to client
//                    System.out.println("Sent to client: " + response);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void run() {
        receiveMessage();
    }
}
