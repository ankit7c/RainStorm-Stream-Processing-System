package org.example.service.FailureDetector;

import org.example.entities.FDProperties;
import org.example.entities.Member;
import org.example.entities.MembershipList;
import org.example.entities.Message;
import org.example.service.Ping.PingSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

/**
 * This is the main failure detector server
 */
public class FDServer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(FDServer.class);
    private Dissemination dissemination;

    public FDServer(Dissemination dissemination) {
        this.dissemination = dissemination;
    }
    public void run() {
        while (true) {
            MembershipList.printMembers();
            MembershipList.generateRandomList();
            try {
                while (MembershipList.isLast()) {
                    LocalDateTime startTime = LocalDateTime.now();
                    //Go through the list and is we don't receive an ack for any suspected node in our list then mark it as failed and send a multicast.
                    for (Member member : MembershipList.getSuspectedMembers()) {
                        Duration duration = Duration.between(Member.getTimeFromString(member.getDateTime()),
                                Member.getTimeFromString(Member.getLocalDateTime()));
                        if (duration.toMillis() > (int) FDProperties.getFDProperties().get("suspicionProtocolPeriod")) {
                            logger.debug("Member " + member.getName() + " was in suspect for long time. Sending a failed response");
                            MembershipList.failedNodes.add(member.getId());
                            dissemination.sendConfirmMessage(member);
                        }
                    }
                    //Selects a member at random
                    Member member;
                    try {
                        member = MembershipList.getRandomMember();
                    } catch (IndexOutOfBoundsException | NullPointerException i) {
                        throw new RuntimeException(i.getMessage());
                    }
                    logger.debug("Sending ping to Member : " + member.getName());
                    Map<String, Object> messageContent = new HashMap<>();
                    messageContent.put("messageName", "ping");
                    messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
                    messageContent.put("senderPort", FDProperties.getFDProperties().get("machinePort"));
                    messageContent.put("msgId", FDProperties.generateRandomMessageId());
                    try {
                        Message message = new Message("ping", member.getIpAddress(), member.getPort(), messageContent);
                        //The timeout period for this connection will be (t)
                        PingSender pingSender = new PingSender(message, (int) FDProperties.getFDProperties().get("ackWaitPeriod"));
                        pingSender.start();
                        pingSender.join();
                        String response = pingSender.getResult();

                        if (response.equals("Successful")) {
                            if ((Boolean) FDProperties.getFDProperties().get("isSuspicionModeOn")) {
                                //if the node has been marked as Suspected then ask the Disseminator to spread Alive message
                                logger.info("Member is succescfull but in " + MembershipList.members.get(member.getName()).getStatus());
                                if (MembershipList.members.get(member.getName()).getStatus().equals("Suspected"))
                                    dissemination.sendAliveMessage(member);
                            }
                        } else if (response.equals("Unsuccessful")) {
                            logger.debug("Currently inside FD Server with suspicion mode value " + String.valueOf(FDProperties.getFDProperties().get("isSuspicionModeOn")));
                            if ((Boolean) FDProperties.getFDProperties().get("isSuspicionModeOn")) {
                                //Put the node in the suspect mode and call the disseminator to spread the suspect message
                                if (!member.getStatus().equals("Suspected"))
                                    dissemination.sendSuspectMessage(member);
                            } else {
                                MembershipList.failedNodes.add(member.getId());
                                dissemination.sendFailedMessage(member);
                            }
                        }
                    } catch (UnknownHostException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    //if the total period is greater than 1sec then don't wait, if not then wait for remaining period
                    long time = Duration.between(LocalDateTime.now(), startTime).toMillis();
                    if (time < (int) FDProperties.getFDProperties().get("protocolPeriod")) {
                        try {
                            Thread.sleep((int) FDProperties.getFDProperties().get("protocolPeriod") - time);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                try {
                    Thread.sleep((int) FDProperties.getFDProperties().get("protocolPeriod"));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }
}
