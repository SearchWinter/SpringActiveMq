package com.upchina.activemq.ptp;

/**
 * Created by anjunli on  2024/1/25
 **/

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MultiThreadProducer {

    public static void main(String[] args) {
        // 定义连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", "nio://x.x.x.x:61616");

        // 创建线程数
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new ProducerTask(connectionFactory, i));
            threads[i].start();
        }
    }

    static class ProducerTask implements Runnable {
        private final ActiveMQConnectionFactory connectionFactory;
        private final int threadNumber;

        ProducerTask(ActiveMQConnectionFactory connectionFactory, int threadNumber) {
            this.connectionFactory = connectionFactory;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            Connection connection = null;
            Session session = null;
            MessageProducer producer = null;

            try {
                connection = connectionFactory.createConnection();
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Destination destination = session.createQueue("q_test");
                producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                for (int i = 0; i < 200; i++) {
                    String msg = "Thread-" + threadNumber + ": ActiveMQ test message " + i;
                    producer.send(session.createTextMessage(msg));
                    System.out.println(msg);
                }
            } catch (JMSException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (producer != null) {
                        producer.close();
                    }
                    if (session != null) {
                        session.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

