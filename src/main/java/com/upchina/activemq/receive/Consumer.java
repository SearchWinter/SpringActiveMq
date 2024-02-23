package com.upchina.activemq.receive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upchina.activemq.entity.Book;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jms.JmsProperties;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.upchina.activemq.config.ActiveMQConfig.*;

/**
 * Created by anjunli on  2023/6/15
 **/
@RestController
@RequestMapping("/mq")
public class Consumer {

    @Autowired
    private JmsTemplate jmsTemplate;

    //todo 1、使用JmsTemplate.receive() 接收消息，此方法是一个阻塞方法，直到接收到消息
    @RequestMapping("/receive")
    public void receiveMsg(boolean flag) throws JMSException {
        System.out.println("receiveMsg()......");
//        while (flag) {
        for (; ; ) {
            final Message message = jmsTemplate.receive(QUEUE_STRING);
            if (message instanceof TextMessage) {
                final String text = ((TextMessage) message).getText();
                System.out.println(new Date() + " jsmTemplate receiveMsg: " + text);
            }
        }
    }

    @RequestMapping("/receive2")
    public void receiveMsg2(int batchSize) throws JMSException {
        //设置等待时间，超过时间后没有消息，receive返回null
        jmsTemplate.setReceiveTimeout(1000);
        System.out.println("receiveMsg2()......");
        List<TextMessage> batchList = new ArrayList<TextMessage>(batchSize);
//        while (batchList.size() < batchSize) {
        for (int i = 0; i < batchSize; i++) {
            System.out.println("i = " + i);
            TextMessage message = (TextMessage) jmsTemplate.receive(QUEUE_STRING);
            if (message == null) {
                System.out.println("message = " + message);
                break;
            }
            batchList.add(message);
            System.out.println(new Date() + " jsmTemplate receiveMsg2: " + message.getText());
        }
        //receive之后，程序出现异常，数据已经从队列中取出了
/*        int i=0;
        System.out.println("10/i = " + 10/i);*/
    }

    //接收JsonArray的数据
    @RequestMapping("/receive3")
    public void receiveMsg3(boolean flag) throws JMSException, JsonProcessingException {
        System.out.println("receiveMsg3()......");
        while (flag) {
            final Message message = jmsTemplate.receive(QUEUE_STRING);
            if (message instanceof TextMessage) {
                final String text = ((TextMessage) message).getText();
                //兼容不使用JsonArray的数据
                if (text.startsWith("[")) {
                    // 使用 Jackson 库解析 JSON 字符串为 List
                    ObjectMapper objectMapper = new ObjectMapper();
                    List<String> stringList = Arrays.asList(objectMapper.readValue(text, String[].class));
                    for (String str : stringList) {
                        System.out.println(new Date() + " jsmTemplate receiveMsg3: " + str);
                    }
                } else {
                    System.out.println(new Date() + " jsmTemplate receiveMsg3: " + text);
                }
            }
        }
    }

    //使用JmsTemplate.receive() 接收对象消息
    @RequestMapping("/receiveBook")
    public void receiveBook(int size) throws JMSException {
        System.out.println("receiveMsg()......");
        for (int i = 0; i < size; i++) {
            final Message message = jmsTemplate.receive(QUEUE_BOOK);
            if (message instanceof ObjectMessage) {
                 Object object = ((ObjectMessage) message).getObject();
                System.out.println(new Date() + " jsmTemplate receiveMsg: " + (Book)object);
            }else{
                System.out.println("message = " + message);
            }
        }
    }

    //todo 2、使用@JmsListener注解监听队列，接收消息
/*   @JmsListener(destination = QUEUE_STRING, concurrency = "1")
    public void receiveMsg2(@Payload String msg, @Headers MessageHeaders headers, Message message, Session session) {
        System.out.println(new Date() + " JmsListener receiveMsg2(): " + msg);
        String[] fields = msg.split("\\|", -1);
        System.out.println(fields.length);

        //输出详细信息
//        System.out.println("- - - - - - - - - - - - - - - - - - - - - - - -");
//        System.out.println("######          Message Details           #####");
//        System.out.println("- - - - - - - - - - - - - - - - - - - - - - - -");
//        System.out.println("headers: " + headers);
//        System.out.println("message: " + message);
//        System.out.println("session: " + session);
//        System.out.println("- - - - - - - - - - - - - - - - - - - - - - - -");

    }*/

/*    @JmsListener(destination = QUEUE_STRING2,concurrency = "5")
    public void receiveMsg3(String msg) {
        System.out.println(new Date()+" JmsListener receiveMsg3(): " + msg);
    }*/


    //todo 2、使用@JmsListener注解监听队列，接收消息  验证payload值的属性
    //消费List<Book>数据 ------》消费失败，类型无法转换
    //List<Book> [{Author=A0, page=0}, {Author=A1, page=1}, {Author=A2, page=2}, {Author=A3, page=3}, {Author=A4, page=4}, {Author=A5, page=5}, {Author=A6, page=6}, {Author=A7, page=7}, {Author=A8, page=8}, {Author=A9, page=9}]
    //使用List<String>生产者：对象转JSON字符串，发送List<String> 消费者：从list取数据然后再将该json格式字符串转换成对象
/*    @JmsListener(destination = QUEUE_LIST_BOOK, concurrency = "1")
    public void listenerReceiveMsg2(@Payload List<String> msgs, @Headers MessageHeaders headers, Message message, Session session) {
        System.out.println("listenerReceiveMsg2：" + msgs.size());
        System.out.println(msgs);
        System.out.println(msgs.get(0));

        for (int i = 0; i < msgs.size(); i++) {

            ObjectMapper objectMapper = new ObjectMapper();
            Book book = null;
            try {
                book = objectMapper.readValue(msgs.get(i), Book.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            System.out.println("listenerReceiveMsg2：" + book);
        }

    }*/

    //消费Book数据 ------》消费成功
/*    @JmsListener(destination = QUEUE_BOOK, concurrency = "1")
    public void listenerReceiveBook(@Payload Book msgs, @Headers MessageHeaders headers, Message message, Session session) {

        System.out.println("listenerReceiveMsg3: " + msgs);

    }*/


    //消费List<String>数据 ------》消费成功
/*
    @JmsListener(destination = QUEUE_LIST_STRING, concurrency = "1")
    public void listenerReceiveMsg4(@Payload List<String> msgs, @Headers MessageHeaders headers, Message message, Session session) {
        for (String msg : msgs) {
            System.out.println("listenerReceiveMsg4: " + msg);
        }
    }
*/

}
