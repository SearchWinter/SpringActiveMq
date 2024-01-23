package com.upchina;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by anjunli on  2023/6/15
 **/
@SpringBootApplication(scanBasePackages = "com.upchina")
public class Main {
    public static void main(String[] args) {
        System.setProperty("tomcat.util.http.parser.HttpParser.requestTargetAllow","[]|{}^&#x5c;&#x60;&quot;&lt;&gt;");
        SpringApplication.run(Main.class,args);
    }
}
