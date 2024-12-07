package org.example.Stream;

import org.example.FileSystem.Sender;

import java.util.ArrayList;

public class WorkerManagerListener {
    public void run() {
        while (true) {
            try {
                ArrayList<QueueData> map = WorkerManager.producerQueue.take();
                Sender sender = new Sender();
                for (QueueData d : map) {
                    sender.sendTuple(d.tuple, d.member, d.receiverWorkerId, d.senderWorkerId);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
