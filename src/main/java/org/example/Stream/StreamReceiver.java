package org.example.Stream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

//TODO code to receive tuples to the next node
public class StreamReceiver extends Thread{

    //public static List<Batch> batchesReceived = new CopyOnWriteArrayList<>();

    public void ListenAck(){

    }

    public void receiveBatches() {
        int serverPort = 5010;

        try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
            System.out.println(" Stream Receiver is listening on port " + serverPort);

            while (true) {
                try (Socket socket = serverSocket.accept();
                     ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                    Object receivedObject = objectInputStream.readObject();
                    if (receivedObject instanceof Batch) {
                        Batch receivedBatch = (Batch) receivedObject;
                        Worker.batchesReceived.add(receivedBatch);
                        out.println("Batch " + receivedBatch.getBatchId() + " processed successfully.");
                    } else {
                        System.out.println("Invalid object received.");
                        out.println("Error: Expected Batch object.");
                    }
                } catch (ClassNotFoundException e) {
                    System.err.println("Error deserializing object: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        receiveBatches();
    }
}
