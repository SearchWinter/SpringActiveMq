package com.upchina.activemq.ptp;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import javax.jms.*;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Created by anjunli on  2024/1/23
 * linux 部署activemq服务
 * 同步+事务false
 * 1000row  DeliveryMode.PERSISTENT  48167ms
 * 1000row  DeliveryMode.NON_PERSISTENT  82ms
 * <p>
 * 异步+事务false
 * 1000row  DeliveryMode.PERSISTENT  138ms
 * 1000row  DeliveryMode.NON_PERSISTENT  75ms
 * <p>
 * 同步+事务true
 * 1000row DeliveryMode.PERSISTENT 事务true 187ms
 **/
public class Producer {

    public static void main(String[] args) {
        //定义连接工厂：用于创建链接的工厂类型
        ActiveMQConnectionFactory connectionFactory = null;
        //定义连接
        Connection connection = null;
        //定义会话
        Session session = null;
        //定义消息目的地
        Destination destination = null;
        //定义消息生产者
        MessageProducer mProducer = null;
        //定义消息
        Message message = null;


        try {

            //用户名 密码 访问ActiveMQ服务的路径 结构为: 协议名://主机地址:端口号
            connectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://172.16.8.156:61616");
//            connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
//            connectionFactory.setOptimizeAcknowledge(true);
            //设置回执点，102400byte 异步发送使用
//            connectionFactory.setProducerWindowSize(102400);

            //创建连接
            connection = connectionFactory.createConnection();
            //同步/异步
//            ((ActiveMQConnection)connection).setUseAsyncSend(true);
            //启动连接
            connection.start();
            //创建会话
//            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //使用事务
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //创建目的地，也就是队列名
            destination = session.createQueue("q_test");
            //创建消息生成者，该生成者与目的地绑定
            mProducer = session.createProducer(destination);

            //投递模式，是否持久化
            mProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            //创建消息
//            message = session.createTextMessage("ActiveMQ test");
            //发送消息
//            mProducer.send(message);
            String msg = "172.16.3.38|2024-01-22 23:59:59|81d9142b0bcd01f5d4969fd5b79d14c3|SCRW22020701G0325|SN=WIN_com.sandbox.client.windows&VN=0_5.4.1_445_DD&RL=1920_1080&OS=6.1.7601&CHID=4008_4005&MN=stockpc&SDK=1.0.1&MO=&VC=&RV=|50EBF6B33A58|0|8248_1705939199|222.214.187.233||active|0||5|0|0|WIN_COM.SANDBOX.CLIENT.WINDOWS|0_5.4.1_445_DD|4008_4005|";
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                mProducer.send(session.createTextMessage(msg + i));
            }
            //手动提交事务
            session.commit();
            System.out.println(System.currentTimeMillis() - start);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (mProducer != null) {
                try {
                    mProducer.close();
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
