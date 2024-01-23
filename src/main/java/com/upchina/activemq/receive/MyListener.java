package com.upchina.activemq.receive;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Date;

/**
 * Created by anjunli on  2021/9/7
 * 使用ActiveMQ监听器来监听队列，持续消费消息。
 **/
public class MyListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            Date date = new Date();
            System.out.println(date+" "+textMessage.getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
