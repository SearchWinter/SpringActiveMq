package com.upchina.activemq.ptp;

import com.upchina.activemq.entity.Book;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQObjectMessage;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by anjunli on  2024/2/2
 **/

public class TestBook {
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
            Book book = new Book("a" + i, i);
            ObjectMessage objectMessage = session.createObjectMessage(book);
            //7.生产者发送消息
            producer.send(objectMessage);
        }
        //8.提交事务
        session.commit();
        //9.关闭资源
        session.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("totalcost  =" + (end - start));
    }


    public static List<Book> batchConsumer(int batchSize, String queueName) throws JMSException {
        //1.创建connectionfacoty
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://x.x.x.x:61616");
        //设置要取消序列化的受信任包的列表
        connectionFactory.setTrustedPackages(new ArrayList(Arrays.asList("com.upchina.activemq.entity".split(","))));

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
        List<Book> batchList = new ArrayList<Book>(batchSize);
        while (batchList.size() < batchSize) {
            ActiveMQObjectMessage message = (ActiveMQObjectMessage) consumer.receive(1000);
            if (message == null) {
                break;
            }
            batchList.add((Book) message.getObject());
        }
        System.out.println("batchList = " + batchList.size());
        session.commit();
//        session.rollback();
        //9.关闭资源
        session.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("totalcost  =" + (end - start));
        return batchList;
    }

    public static void main(String[] args) throws Exception {
//        batchProducer(1000, "q_test3");
        List<Book> messages = batchConsumer(1000, "q_test3");
        for (Book message : messages) {
            System.out.println(message);
        }
    }
}
