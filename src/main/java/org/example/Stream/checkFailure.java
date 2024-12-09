package org.example.Stream;

public class checkFailure extends Thread {
    public void run() {
        while (true) {
            try {
                if (!Leader.failedNodes.isEmpty()) {
                    Thread.sleep(10000);
                    Leader.updateFailedNode();
                }
                Thread.sleep(2000);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
