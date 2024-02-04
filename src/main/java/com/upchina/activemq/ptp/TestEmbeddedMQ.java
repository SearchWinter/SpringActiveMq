package com.upchina.activemq.ptp;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import javax.jms.*;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestEmbeddedMQ {
    static {
        final BrokerService broker = new BrokerService();
        try {
            broker.addConnector("vm://localhost");
            PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
            Path path = Paths.get("kaha");
            File dir = new File(path.toUri());
            if (!dir.exists()) {
                dir.mkdirs();
            }
            persistenceAdapter.setDirectory(dir);
            broker.setPersistenceAdapter(persistenceAdapter);
            broker.setPersistent(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void batchProducer(int produceSize) throws Exception {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue("tempqueue");
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageProducer producer = session.createProducer(destination);

        long start = System.currentTimeMillis();
        for (int i = 0; i < produceSize; i++) {
            //6.使用session创建消息，这里使用TEXT类型的消息
            TextMessage textMessage = session.createTextMessage(UUID.randomUUID().toString());
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

    public static void consumer() throws JMSException {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue("tempqueue");
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageConsumer consumer = session.createConsumer(destination);
        Message message = consumer.receive();
        System.out.println(message);
        session.rollback();
        //9.关闭资源
        session.close();
        connection.close();
    }

    public static List<TextMessage> batchConsumer(int batchSize) throws JMSException {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue("tempqueue");
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageConsumer consumer = session.createConsumer(destination);

        long start = System.currentTimeMillis();
        List<TextMessage> batchList = new ArrayList<TextMessage>(batchSize);
        while (batchList.size() < batchSize) {
            TextMessage message = (TextMessage) consumer.receive(10000);
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
        batchProducer(1000);
//        List<TextMessage> messages = batchConsumer(10000);
//        System.out.println("messages = " + messages.size());
/*        for (TextMessage message : messages) {
            System.out.println(message.getText());
        }*/
    }
}
