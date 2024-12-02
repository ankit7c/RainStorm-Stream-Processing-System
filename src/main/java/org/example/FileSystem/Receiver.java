package org.example.FileSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Stream.Worker;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

                    System.out.println("Client connected");

                    // Read message from client
                    String received = in.readLine();
                    Message message = Message.process(socket.getInetAddress(), String.valueOf(socket.getPort()), received);
                    System.out.println("Received from client: " + message);
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
                            // TODO add code below to Receive control commands
                        case "control":
                            // TODO based on the control message: check what leader has given us a role
                            // TODO based on the role take appropriate action

                            // TODO if a node fails and you are a source then repeat the tuples
                            // Add code below to handle failures
                        case "set_source":
                            //Action a source needs to take when a Leader asks to perform a task
                            try {
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                List<Member> op1s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op1s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op1_" + i)))));
                                }
                                String range = String.valueOf(message.getMessageContent().get("range"));
                                String filename = String.valueOf(message.getMessageContent().get("filename"));
                                Worker worker = new Worker("source", null, op1s, null, range, filename, null);
                                worker.start();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "set_op1":
                            //Action a OP1 needs to take when a Leader asks to perform a task
                            try {
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                List<Member> sources = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    sources.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("source_" + i)))));
                                }
                                List<Member> op2s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    sources.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op2_" + i)))));
                                }
                                Worker worker = new Worker("set_op1", sources, null, op2s, null, null, null);
                                worker.start();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "set_op2":
                            //Action a OP2 needs to take when a Leader asks to perform a task
                            try {
                                int tot_ops = Integer.parseInt(String.valueOf(message.getMessageContent().get("num_tasks")));
                                List<Member> op1s = new ArrayList<>();
                                for (int i = 0; i < tot_ops; i++) {
                                    op1s.add(MembershipList.memberslist.get(Integer.parseInt(String.valueOf(message.getMessageContent().get("op1_" + i)))));
                                }
                                String destFilename = String.valueOf(message.getMessageContent().get("filename"));
                                Worker worker = new Worker("set_op2", null, op1s, null, null, null, destFilename);
                                worker.start();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "BatchAck":
                            String batchId = String.valueOf(message.getMessageContent().get("batchId"));
                            //TODO ask saurabh

                        case "/exit":
                    }
                    // Send response back to client
                    System.out.println("Sent to client: " + response);
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
