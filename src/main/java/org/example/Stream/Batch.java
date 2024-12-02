package org.example.Stream;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Batch implements Serializable {
    private static final long serialVersionUID = 1L;
    private String batchId;
    private int senderMachineId ;
    private List<Tuple> BatchData = new CopyOnWriteArrayList<>();


    public Batch(String batchId, int senderMachineId, List<Tuple> batchData) {
        this.batchId = batchId;
        this.senderMachineId = senderMachineId;
        this.BatchData = batchData;
    }

    public Batch(String batchId, List<Tuple> batchData) {
        this.batchId = batchId;
        this.BatchData = batchData;
    }

    public int getSenderMachineId() {
        return senderMachineId;
    }

    public void setSenderMachineId(int senderMachineId) {
        this.senderMachineId = senderMachineId;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public List<Tuple> getBatchData() {
        return BatchData;
    }

    public void setBatchData(List<Tuple> batchData) {
        BatchData = batchData;
    }


}
