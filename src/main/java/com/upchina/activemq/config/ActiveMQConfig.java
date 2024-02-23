package com.upchina.activemq.config;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.catalina.connector.Connector;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.stereotype.Service;
import org.springframework.util.ErrorHandler;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

@EnableJms
@Configuration
public class ActiveMQConfig {

    public static final String QUEUE_STRING = "queue_string";
    public static final String QUEUE_STRING2 = "queue_string2";
    public static final String QUEUE_LIST_STRING = "queue_list_string";
    public static final String QUEUE_CK = "queue_ck";
    public static final String QUEUE_BOOK = "queue_book";
    public static final String QUEUE_LIST_BOOK = "queue_list_book";
    public static final String BROKER_URL = "vm://localhost";
//    public static final String BROKER_URL = "tcp://localhost:61616?jms.useAsyncSend=true";

    //本地jvm嵌入ActiveMQ使用，与application.properties里面的外部ActiveMQ配置，选择一个使用
   /* @Bean
    public JmsListenerContainerFactory<?> queueListenerFactory(JmsErrorHandler errorHandler) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setMessageConverter(messageConverter());
        factory.setErrorHandler(errorHandler);
        return factory;
    }

    @Service
    public static class JmsErrorHandler implements ErrorHandler {
        @Override
        public void handleError(Throwable t) {
//            t.printStackTrace();
            System.out.println("empty msg");
        }
    }

    //将消息转换为TextMessage
    @Bean
    public MessageConverter messageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public BrokerService broker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.addConnector(BROKER_URL);
        PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();

        Path path = Paths.get("D://kaha");
        File dir = new File(path.toUri());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        persistenceAdapter.setDirectory(dir);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setPersistent(true);
//        broker.setPersistent(false);
        return broker;
    }*/

//添加受信任的包
/*    @Bean
    public JmsTemplate jmsTemplate() {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        factory.setTrustedPackages(new ArrayList(Arrays.asList("com.upchina.activemq.entity".split(","))));
        return new JmsTemplate(factory);
    }*/

    //允许请求中带特殊符号
    @Bean
    public TomcatServletWebServerFactory webServerFactory() {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.addConnectorCustomizers(new TomcatConnectorCustomizer() {
            @Override
            public void customize(Connector connector) {
                // 配置Spring boot支持在URL请求参数中加{}特殊字符
                connector.setProperty("relaxedPathChars", "|{}");
                connector.setProperty("relaxedQueryChars", "|{}");
            }
        });
        return factory;
    }

}