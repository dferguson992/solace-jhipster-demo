package com.ippon.dferguson.service;

import com.solacesystems.jcsmp.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class SolacePublisher {

    final JCSMPProperties properties = new JCSMPProperties();
    private JCSMPSession session = null;

    public SolacePublisher() throws JCSMPException {
        properties.setProperty(JCSMPProperties.HOST, "tcp://mr1u6o37qngl6d.messaging.solace.cloud:20992");
        properties.setProperty(JCSMPProperties.USERNAME, "solace-cloud-client");
        properties.setProperty(JCSMPProperties.PASSWORD, "heb20tjthifn31dbss6l44fqaj");
        properties.setProperty(JCSMPProperties.VPN_NAME, "msgvpn-244g85za2e2t");
    }

    public void connect() throws JCSMPException {
        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
    }

    boolean isOpen(){
        return !session.isClosed();
    }

    public void disconnect() {
        if(session != null && !session.isClosed()) {
            session.closeSession();
            session = null;
        }
    }

    public void publish(String payload, String publish_topic) throws JCSMPException {
        if(isOpen()){
            XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
                @Override
                public void responseReceived(String messageID) {
                    System.out.println("Producer received response for msg: " + messageID);
                }
                @Override
                public void handleError(String messageID, JCSMPException e, long timestamp) {
                    System.out.printf("Producer received error for msg: %s@%s - %s%n",
                        messageID,timestamp,e);
                }
            });
            final Topic topic = JCSMPFactory.onlyInstance().createTopic(publish_topic);
            TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
            msg.setText(payload);
            prod.send(msg,topic);
            prod.close();
        }
    }
}
