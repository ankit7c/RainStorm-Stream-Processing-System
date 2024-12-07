package org.example.Stream;

import org.example.entities.Member;

public class QueueData {
    int senderWorkerId;
    int receiverWorkerId;
    Member member;
    Tuple tuple;
    String type;
    String tupleId;

    public QueueData(int senderWorkerId, int receiverWorkerId, Member member, Tuple tuple, String type, String tupleId) {
        this.senderWorkerId = senderWorkerId;
        this.receiverWorkerId = receiverWorkerId;
        this.member = member;
        this.tuple = tuple;
        this.type = type;
        this.tupleId = tupleId;
    }

    public QueueData(int senderWorkerId, int receiverWorkerId, Member member, String type, String tupleId) {
        this.senderWorkerId = senderWorkerId;
        this.receiverWorkerId = receiverWorkerId;
        this.member = member;
        this.type = type;
        this.tupleId = tupleId;
    }


}
