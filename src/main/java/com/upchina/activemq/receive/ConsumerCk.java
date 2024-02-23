package com.upchina.activemq.receive;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.sql.*;
import java.util.Date;
import java.util.List;

import static com.upchina.activemq.config.ActiveMQConfig.*;

/**
 * Created by anjunli on  2023/6/15
 **/
@RestController
@RequestMapping("/ck")
public class ConsumerCk {

    @Autowired
    public JmsTemplate jmsTemplate;

    //todo 1、使用JmsTemplate.receive() 接收消息，此方法是一个阻塞方法，直到接收到消息
    @RequestMapping("/receive")
    public void receiveMsg(boolean flag) throws JMSException {
        System.out.println("receiveMsg()......");
//        while (flag) {
        for (; ; ) {
            Message message = jmsTemplate.receive(QUEUE_CK);
            if (message instanceof TextMessage) {
                String text = ((TextMessage) message).getText();
                String[] split = text.split("\\|", -1);
                // cd_807ce734972a|d636a95ea8365351ec0f16a742dbea97|117|lc_xff|57|2|0
                String sql = "insert into t_material_stat(uid,guid,put_id,position_id,material_id,platform,action,put_time,UPDATETIME) values(?,?,?,?,?,?,?,?,?)";
                try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://xxx.xxx.xxx.xxx:8123/db_base_log_stat", "xxx", "xxx");
                     PreparedStatement psmt = connection.prepareStatement(sql);) {

                    psmt.setString(1, split[0]);
                    psmt.setString(2, split[1]);
                    psmt.setInt(3, Integer.parseInt(split[2]));
                    psmt.setString(4, split[3]);
                    psmt.setInt(5, Integer.parseInt(split[4]));
                    psmt.setInt(6, Integer.parseInt(split[5]));
                    psmt.setInt(7, Integer.parseInt(split[6].replaceAll("\"","")));
                    psmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                    psmt.setString(9, "2024-01-08 14:46:24");
                    psmt.execute();

                } catch (SQLException e) {
                    e.printStackTrace();
                }
                System.out.println(new Date() + " jsmTemplate receiveMsg: " + text);
            }
        }
    }


    //todo 2、使用@JmsListener注解监听队列，接收消息
/*   @JmsListener(destination = LOG_QUEUE_CK, concurrency = "1")
    public void receiveMsg2(@Payload String msg) {
       String[] split = msg.split("\\|", -1);
       // cd_807ce734972a|d636a95ea8365351ec0f16a742dbea97|117|lc_xff|57|2|0
       String sql = "insert into t_material_stat(uid,guid,put_id,position_id,material_id,platform,action,put_time,UPDATETIME) values(?,?,?,?,?,?,?,?,?)";
       try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://xxx.xxx.xxx.xxx:8123/db_base_log_stat", "xxx", "xxx");
            PreparedStatement psmt = connection.prepareStatement(sql);) {

           psmt.setString(1, split[0]);
           psmt.setString(2, split[1]);
           psmt.setInt(3, Integer.parseInt(split[2]));
           psmt.setString(4, split[3]);
           psmt.setInt(5, Integer.parseInt(split[4]));
           psmt.setInt(6, Integer.parseInt(split[5]));
           psmt.setInt(7, Integer.parseInt(split[6].replaceAll("\"","")));
           psmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
           psmt.setString(9, "2024-01-08 14:46:24");
           psmt.execute();

       } catch (SQLException e) {
           e.printStackTrace();
       }
       System.out.println(new Date() + " jJmsListener receiveMsg3:: " + msg);
    }*/

    @JmsListener(destination = QUEUE_CK, concurrency = "1")
    public void receiveMsg3(@Payload List<String> list) {
        // cd_807ce734972a|d636a95ea8365351ec0f16a742dbea97|117|lc_xff|57|2|0
        String sql = "insert into t_material_stat(uid,guid,put_id,position_id,material_id,platform,action,put_time,UPDATETIME) values(?,?,?,?,?,?,?,?,?)";
        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://xxx.xxx.xxx.xxx:8123/db_base_log_stat", "xxx", "xxx");
             PreparedStatement psmt = connection.prepareStatement(sql);) {
            for(String str:list) {
                System.out.println(new Date() + " JmsListener receiveMsg3: " + str);
                String[] split = str.split("\\|", -1);
                psmt.setString(1, split[0]);
                psmt.setString(2, split[1]);
                psmt.setInt(3, Integer.parseInt(split[2]));
                psmt.setString(4, split[3]);
                psmt.setInt(5, Integer.parseInt(split[4]));
                psmt.setInt(6, Integer.parseInt(split[5]));
                psmt.setInt(7, Integer.parseInt(split[6].replaceAll("\"", "")));
                psmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                psmt.setString(9, "2024-01-08 14:46:24");
                psmt.addBatch();
            }
            psmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
