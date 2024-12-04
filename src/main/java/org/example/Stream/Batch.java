package org.example.Stream;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Batch implements Serializable {
    private static final long serialVersionUID = 1L;
    private String batchId;
    private int senderWorkerId;
    private int receiverWorkerId;
    private List<Tuple> BatchData = new CopyOnWriteArrayList<>();



    public Batch(String batchId, int senderWorkerId, List<Tuple> batchData) {
        this.batchId = batchId;
        this.senderWorkerId = senderWorkerId;
        this.BatchData = batchData;
    }

    public Batch(String batchId, int senderWorkerId, int receiverWorkerId, List<Tuple> batchData) {
        this.batchId = batchId;
        this.senderWorkerId = senderWorkerId;
        this.receiverWorkerId = receiverWorkerId;
        this.BatchData = batchData;
    }

    public Batch(String batchId, List<Tuple> batchData) {
        this.batchId = batchId;
        this.BatchData = batchData;
    }

    public int getSenderWorkerId() {
        return senderWorkerId;
    }

    public void setSenderWorkerId(int senderWorkerId) {
        this.senderWorkerId = senderWorkerId;
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

    public int getReceiverWorkerId() {
        return receiverWorkerId;
    }

    public void setReceiverWorkerId(int receiverWorkerId) {
        this.receiverWorkerId = receiverWorkerId;
    }


}
