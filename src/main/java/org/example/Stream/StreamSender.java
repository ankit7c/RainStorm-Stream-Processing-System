package org.example.Stream;

import org.example.entities.FDProperties;
import org.example.entities.Member;
import org.example.entities.MembershipList;
import org.example.entities.Message;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

//TODO code to send tuples to the next node
public class StreamSender extends Thread{

    private Worker worker;



    public StreamSender(Worker worker) {
        this.worker = worker;
    }

    private void sendBatches() {
        while (true) {
            synchronized (worker.batchesToBeSent) {
                if (worker.batchesToBeSent.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("StreamSender thread interrupted: " + e.getMessage());
                        break;
                    }
                    continue;
                }

                for(Batch batchToSend : worker.batchesToBeSent){
                    try {
                        int receiverPort = worker.receiverPorts.get(batchToSend.getReceiverWorkerId());
                        String receiverIp = "";
                        if(worker.op1s.containsKey(batchToSend.getReceiverWorkerId())){
                            Member member = worker.op1s.get(batchToSend.getReceiverWorkerId());
                            receiverIp = member.getIpAddress();
                        }else if(worker.op2s.containsKey(batchToSend.getReceiverWorkerId())){
                            Member member = worker.op2s.get(batchToSend.getReceiverWorkerId());
                            receiverIp = member.getIpAddress();
                        }
                        Socket socket = new Socket(receiverIp, receiverPort);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(batchToSend);
                        outputStream.flush();
                        worker.batchesSent.add(batchToSend);
                        worker.batchesToBeSent.remove(batchToSend);
                    } catch (IOException e) {
                        System.err.println("Error sending batch " + batchToSend.getBatchId() + " to receiver " + batchToSend.getReceiverWorkerId());

                    }
                }
            }
        }
    }


    @Override
    public void run() {
        sendBatches();

    }

}
