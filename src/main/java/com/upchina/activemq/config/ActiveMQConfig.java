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
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.stereotype.Service;
import org.springframework.util.ErrorHandler;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

@EnableJms
@Configuration
public class ActiveMQConfig {

    public static final String LOG_QUEUE = "log_queue";
    public static final String LOG_QUEUE2 = "log_queue2";
    public static final String LOG_QUEUE_CK = "log_queue_ck";

    @Bean
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
        broker.addConnector("vm://localhost");
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
    }


    //允许请求中带特殊符号
    @Bean
    public TomcatServletWebServerFactory webServerFactory() {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.addConnectorCustomizers(new TomcatConnectorCustomizer() {
            @Override
            public void customize(Connector connector) {
                // 配置Spring boot支持在URL请求参数中加{}特殊字符
                connector.setProperty("relaxedPathChars","|{}");
                connector.setProperty("relaxedQueryChars", "|{}");
            }
        });
        return factory;
    }

}