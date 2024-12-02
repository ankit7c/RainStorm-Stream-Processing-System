package org.example.Stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

//TODO code to send tuples to the next node
public class StreamSender extends Thread{

//    public static List<Batch> batchesSent = new CopyOnWriteArrayList<>();
//    public static List<Batch> batchesToBeSent= new CopyOnWriteArrayList<>();
//
//    public void sendBatchData(){
//
//    }
    public String sendBatch(String ipAddress, int port, Batch batch) {
        try (Socket socket = new Socket(ipAddress, port);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send the serialized Batch object
            objectOutputStream.writeObject(batch);

            // Read response from the receiver
            return in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return "Unsuccessful";
        }
    }

//    @Override
//    public void run() {
//        if(Worker.batchesToBeSent!=null && !Worker.batchesToBeSent.isEmpty()){
//
//        }
//    }

}
