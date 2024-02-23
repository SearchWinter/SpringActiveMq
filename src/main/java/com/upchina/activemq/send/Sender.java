package com.upchina.activemq.send;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upchina.activemq.entity.Book;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jms.JmsProperties;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.DeliveryMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.upchina.activemq.config.ActiveMQConfig.*;

/**
 * Created by anjunli on  2023/6/15
 **/
@RestController
@RequestMapping("/mq")
public class Sender {

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Autowired
    private JmsTemplate jmsTemplate;

    @PostMapping("/send")
    public void sendMsg(String msg, int size) {
        final long start = System.currentTimeMillis();
        System.out.println("send msg: " + msg);
        if (msg.isEmpty()) {
            return;
        }
        //消息确认
        jmsTemplate.setSessionAcknowledgeMode(JmsProperties.AcknowledgeMode.CLIENT.getMode());
/*        //JmsTemplate 默认参数,设置为true，deliveryMode, priority, timeToLive等设置才会起作用
        jmsTemplate.setExplicitQosEnabled(Boolean.TRUE);
        //设为非持久化模式
        jmsTemplate.setDeliveryMode(DeliveryMode.PERSISTENT);*/
        for (int i = 0; i < size; i++) {
            System.out.println(i);
//            if (i % 2 != 0) {
            if (i >= 0) {
                jmsTemplate.convertAndSend(QUEUE_STRING, msg);
            } else {
                jmsTemplate.convertAndSend(QUEUE_STRING2, msg);
            }
        }
        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }

    @PostMapping("/send2")
    public void sendMsg2(String msg, int size) {
        final long start = System.currentTimeMillis();
        System.out.println("send msg: " + msg);
        for (int i = 0; i < size; i++) {
            System.out.println(i);
            int finalI = i;
            //不建议使用
            //不阻塞主线程，但是持久化速度并没有变快，快速关掉程序，一部分数据没有发送到队列；当数据量很大的时候，机器CPU、内存负载会很高
            executorService.execute(() -> {
                jmsTemplate.convertAndSend(QUEUE_STRING, finalI + msg);
            });
        }
        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }

    @PostMapping("/send3")
    public void sendMsg3(String msg, int size) {
        final long start = System.currentTimeMillis();
        System.out.println("send msg: " + msg);
        if (msg.isEmpty()) {
            return;
        }
        int count = 0;
        JSONArray jsonArray = new JSONArray(10);
        for (int i = 0; i < size; i++) {
            System.out.println(i);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("log", msg);
            jsonArray.put(count, jsonObject);
            count++;
            if (count >= 9) {
                jmsTemplate.convertAndSend(QUEUE_STRING, jsonArray.toString());
                jsonArray.clear();
                count = 0;
            }
        }
        if (count > 0) {
            jmsTemplate.convertAndSend(QUEUE_STRING, jsonArray.toString());
        }

        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }


    //发送List<String>对象
    private List<String> list = new ArrayList<>();

    @PostMapping("/send4")
    public void sendMs4(String msg, int size) {
        final long start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            list.add(msg);
            if (list.size() == 10) {
                jmsTemplate.convertAndSend(QUEUE_LIST_STRING, list);
                list.clear();
            }
        }
        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }

    private List<String> bookList = new ArrayList<String>();

    //发送List<Book>对象
    @PostMapping("/send5")
    public void sendMs5(int size) {
        final long start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            Book book = new Book("A" + i, i);

            ObjectMapper objectMapper = new ObjectMapper();
            String json = null;
            try {
                json = objectMapper.writeValueAsString(book);
                bookList.add(json);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (bookList.size() == 10) {
                jmsTemplate.convertAndSend(QUEUE_LIST_BOOK, bookList);
                bookList.clear();
            }
        }
        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }

    //发送单个Book对象
    @PostMapping("/sendBook")
    public void sendBook(int size) {
        final long start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            jmsTemplate.convertAndSend(QUEUE_BOOK, new Book("A" + i, i));
        }
        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }

}
