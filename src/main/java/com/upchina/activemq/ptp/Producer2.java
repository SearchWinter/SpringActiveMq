package com.upchina.activemq.ptp;

import org.apache.activemq.*;

import javax.jms.*;

/**
 * Created by anjunli on  2024/2/20
 * 用连接创建一个MessageProducer，一直用这个生产者
 * setProducerWindowSize：https://activemq.apache.org/components/classic/documentation/producer-flow-control
 **/
public class Producer2 {

    private ActiveMQMessageProducer producer;
    private Session session = null;

    public Producer2() {
        Connection connection = null;
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://x.x.x.x:61616");
            //发送多少字节后，等待broker的确认，才继续发送
            connectionFactory.setProducerWindowSize(10);
            connection = connectionFactory.createConnection();

            //异步发送
            ((ActiveMQConnection)connection).setUseAsyncSend(Boolean.TRUE);

            connection.start();
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("q_test");
            producer = (ActiveMQMessageProducer)session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String msg) {
        try {
            TextMessage message = session.createTextMessage(msg);
            producer.send(message);
            //异步发送消息接收回执，速度慢
/*            producer.send(message, new AsyncCallback() {
                @Override
                public void onSuccess() {
                    System.out.println("消息发送成功");
                }

                @Override
                public void onException(JMSException exception) {
                    System.out.println("消息发送失败");
                }
            });*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws JMSException {
        Producer2 producer2 = new Producer2();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            producer2.sendMessage("Message " + i);
//            producer2.session.commit();
        }
        System.out.println("Time taken: " + (System.currentTimeMillis() - start) + " ms");
    }
}
