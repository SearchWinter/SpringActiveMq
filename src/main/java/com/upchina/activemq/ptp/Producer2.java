package com.upchina.activemq.ptp;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Created by anjunli on  2024/2/20
 * 用连接创建一个MessageProducer，一直用这个生产者
 **/
public class Producer2 {

    private MessageProducer producer;
    private Session session = null;

    public Producer2() {
        Connection connection = null;
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("x", "x", "tcp://x.x.x.x:61616");
            connection = connectionFactory.createConnection();

            //异步发送
//            ((ActiveMQConnection)connection).setUseAsyncSend(Boolean.TRUE);

            connection.start();
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("q_test");
            producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String msg) {
        try {
            TextMessage message = session.createTextMessage(msg);
            producer.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws JMSException {
        Producer2 producer = new Producer2();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            producer.sendMessage("Message " + i);
//            producer.session.commit();
        }
        System.out.println("Time taken: " + (System.currentTimeMillis() - start) + " ms");
    }
}
