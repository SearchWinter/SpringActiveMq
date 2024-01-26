package com.upchina.activemq.ptp;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.jms.JmsException;

import javax.jms.*;

/**
 * Created by anjunli on  2024/1/23
 **/
public class Consumer {
    public static void main(String[] args) {
        //定义连接工厂：用于创建链接的工厂类型
        ConnectionFactory connectionFactory = null;
        //定义连接
        Connection connection = null;
        //定义会话
        Session session = null;
        //定义消息目的地
        Destination destination = null;
        //定义消息生产者
        MessageConsumer mConsumer = null;
        //定义消息
        Message message = null;

        try {
//            connectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://x.x.x.x:61616");
            connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue("q_test");
            mConsumer = session.createConsumer(destination);

            long start = System.currentTimeMillis();
            while (true) {
                message = mConsumer.receive();
                String msg = ((TextMessage) message).getText();
                System.out.println(msg);
                System.out.println(System.currentTimeMillis()-start);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (mConsumer != null) {
                try {
                    mConsumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
