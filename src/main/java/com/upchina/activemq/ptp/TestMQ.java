package com.upchina.activemq.ptp;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestMQ {
    public static void batchProducer(int produceSize, String queueName) throws Exception {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://x.x.x.x:61616");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue(queueName);
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageProducer producer = session.createProducer(destination);

        long start = System.currentTimeMillis();
        for (int i = 0; i < produceSize; i++) {
            //6.使用session创建消息，这里使用TEXT类型的消息
            String msg = "172.16.3.38|2024-01-22 23:59:59|81d9142b0bcd01f5d4969fd5b79d14c3|SCRW22020701G0325|SN=WIN_com.sandbox.client.windows&VN=0_5.4.1_445_DD&RL=1920_1080&OS=6.1.7601&CHID=4008_4005&MN=stockpc&SDK=1.0.1&MO=&VC=&RV=|50EBF6B33A58|0|8248_1705939199|222.214.187.233||active|0||5|0|0|WIN_COM.SANDBOX.CLIENT.WINDOWS|0_5.4.1_445_DD|4008_4005|";
//            TextMessage textMessage = session.createTextMessage(UUID.randomUUID().toString());
            TextMessage textMessage = session.createTextMessage(msg);
            //7.生产者发送消息
            producer.send(textMessage);
        }
        //8.提交事务
        session.commit();
        //9.关闭资源
        session.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("totalcost  =" + (end - start) );
    }


    public static List<TextMessage> batchConsumer(int batchSize, String queueName) throws JMSException {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://x.x.x.x:61616");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue(queueName);
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageConsumer consumer = session.createConsumer(destination);
        long start = System.currentTimeMillis();
        List<TextMessage> batchList = new ArrayList<TextMessage>(batchSize);
        while (batchList.size() < batchSize) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            if (message == null) {
                break;
            }
            batchList.add(message);
        }
        System.out.println("batchList = " + batchList.size());
        session.commit();
//        session.rollback();
        //9.关闭资源
        session.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("totalcost  =" + (end - start) );
        return batchList;
    }

    public static void main(String[] args) throws Exception {
//        batchProducer(1000, "q_test2");
        List<TextMessage> messages = batchConsumer(1000, "q_test2");
/*        for (TextMessage message : messages) {
            System.out.println(message.getText());
        }*/
    }
}
