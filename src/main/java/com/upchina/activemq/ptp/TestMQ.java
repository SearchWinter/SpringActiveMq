package com.upchina.activemq.ptp;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.junit.Test;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestMQ {
    private static final String BROKER_URL = "tcp://x.x.x.x:61616"; // ActiveMQ 服务的地址
    //    private static final String BROKER_URL = "tcp://x.x.x.x:61616?jms.prefetchPolicy.all=50"; // 修改所有消费者类型的预取极限（prefetch limit
//    private static final String BROKER_URL = "tcp://x.x.x.x:61616?jms.prefetchPolicy.queuePrefetch=1"; // 仅设置队列类型消费者的预取极限（prefetch limit）
    private static PooledConnectionFactory pooledConnectionFactory;

    /**
     * 使用同一个Producer一直发消息，手动管理事务，能大幅度提升速度
     */
    public static void batchProducer(int produceSize, String queueName) throws Exception {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://x.x.x.x:61616");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
//        Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
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
        System.out.println("cost time =" + (end - start));
    }


    public void singleProducer(String queueName) throws JMSException {
        long start = System.currentTimeMillis();
        //使用pooledConnectionFactory
//        Connection connection = pooledConnectionFactory.createConnection();

        //1.创建connectionfacoty
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://x.x.x.x:61616");
        //2.使用ConnectionFactory创建connnect,并启动connnect
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setUseAsyncSend(true);
        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue(queueName);
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageProducer producer = session.createProducer(destination);

        //6.使用session创建消息，这里使用TEXT类型的消息
        String msg = "172.16.3.38|2024-01-22 23:59:59|81d9142b0bcd01f5d4969fd5b79d14c3|SCRW22020701G0325|SN=WIN_com.sandbox.client.windows&VN=0_5.4.1_445_DD&RL=1920_1080&OS=6.1.7601&CHID=4008_4005&MN=stockpc&SDK=1.0.1&MO=&VC=&RV=|50EBF6B33A58|0|8248_1705939199|222.214.187.233||active|0||5|0|0|WIN_COM.SANDBOX.CLIENT.WINDOWS|0_5.4.1_445_DD|4008_4005|";
//            TextMessage textMessage = session.createTextMessage(UUID.randomUUID().toString());
        TextMessage textMessage = session.createTextMessage(msg);
        //7.生产者发送消息
        producer.send(textMessage);
        //8.提交事务
//        session.commit();
        //9.关闭资源
        session.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("cost time =" + (end - start));
    }

    public static List<TextMessage> batchConsumer(int batchSize, String queueName) throws JMSException {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
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
        System.out.println("cost time =" + (end - start));
        return batchList;
    }

    public void singleConsumer(String queueName) throws JMSException {

        Connection connection = pooledConnectionFactory.createConnection();

/*        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();*/

        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
        Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue(queueName);
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageConsumer consumer = session.createConsumer(destination);
        long start = System.currentTimeMillis();
        TextMessage message = (TextMessage) consumer.receive(1000);
        System.out.println("message = " + message.getText());
        //6.提交事务
//        session.commit();
//        session.rollback();
        //9.关闭资源ActiveMQMessageConsumer
        session.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("cost time =" + (end - start));
    }


    @Test
    public void test() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        //设置prefetch值
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(1);
        factory.setPrefetchPolicy(prefetchPolicy);

        pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(factory);
        pooledConnectionFactory.setMaxConnections(10);
        pooledConnectionFactory.setIdleTimeout(60 * 1000);

//        pooledConnectionFactory.setIdleTimeout();

        long start = System.currentTimeMillis();
        //批量
//        batchProducer(1000, "q_test2");

/*        List<TextMessage> messages = batchConsumer(1000, "q_test2");
        for (TextMessage message : messages) {
            System.out.println(message.getText());
        }*/

        //单条
        for (int i = 0; i < 1000; i++) {
            singleProducer("q_test3");
        }

/*
        for (int i = 0; i <1000 ; i++) {
            singleConsumer("q_test2");
        }
*/

        long end = System.currentTimeMillis();
        System.out.println("totalcost time =" + (end - start));

        pooledConnectionFactory.stop();
    }

    public static void main(String[] args) throws JMSException {
        System.out.println("args = " + args);

        listenerConsumer();
    }

    /**
     * 监听器
     * 自动确认很快消费完，手动确认消费慢
     */
    public static void listenerConsumer() throws JMSException {
        //1.创建connectionfacoty
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        //2.使用ConnectionFactory创建connnect,并启动connnect
        Connection connection = connectionFactory.createConnection();

        connection.start();
        //3.使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
//        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(这里以PTP为例，所以目的地是一个Queue)，参数是Queue的名字
        Destination destination = session.createQueue("log_queue");
        //5.创建生产者，第一个参数是目的地，此时创建的生产者要与目的地进行绑定。
        MessageConsumer consumer = session.createConsumer(destination);
        //创建监听器
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage txtMsg = (TextMessage) message;
                try {
                    System.out.println("txtMsg = " + txtMsg.getText());
//                    session.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }

/**
 * Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
 * batchProducer() 同一个session发送1000rows数据，session.commit()一次  0.6s
 *
 * batchConsumer() 同一个session接收1000rows数据，session.commit()一次  3s
 *
 * singleProducer()一次发送一条数据
 * 使用PooledConnectionFactory 10个连接 1000rows 39s  不使用，每次新建连接 1000rows 53s
 * 使用PooledConnectionFactory 50个连接 1000rows 33s
 *
 * singleConsumer()一次接收一条数据
 * 使用PooledConnectionFactory 10个连接 1000rows 53s    不使用，每次新建连接 1000rows 68s
 * 使用PooledConnectionFactory 10个连接，prefetch=1 1000rows 41s
 *
 * Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
 *
 * singleProducer()一次发送一条数据
 * 使用PooledConnectionFactory 10个连接 1000rows 37s  不使用，每次新建连接 1000rows 54s
 *
 * singleConsumer()一次接收一条数据
 * 使用PooledConnectionFactory 10个连接 1000rows 29s 不使用，每次新建连接 1000rows 24s
 * 使用PooledConnectionFactory 10个连接 prefetch=1  1000rows 3s
 */

}
