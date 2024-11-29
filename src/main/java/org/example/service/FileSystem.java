package org.example.service;

import org.example.entities.FileTransferManager;

public class FileSystem extends Thread {
    public void run() {
        while (true) {
            try {
                Runnable request = FileTransferManager.getRequestQueue().takeRequest();
//                new Thread(request).start();  // Start each task as a new thread
                request.run(); // will start a task and wait till it gets end

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
