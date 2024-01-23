package com.upchina.activemq.send;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



/**
 * Created by anjunli on  2024/1/23
 **/
@RestController
@RequestMapping("/mq2")
public class Sender2 {

    @PostMapping("/send")
    public void sendMsg(String msg, int size) {
        final long start = System.currentTimeMillis();
        System.out.println("send msg: " + msg);
        if (msg.isEmpty()) {
            return;
        }
        for (int i = 0; i < size; i++) {
            System.out.println(i);

        }
        final long end = System.currentTimeMillis();
        System.out.println("start: " + start + " end: " + end + " cost: " + (end - start));

    }
}
