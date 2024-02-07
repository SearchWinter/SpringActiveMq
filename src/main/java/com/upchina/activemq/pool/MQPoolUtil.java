package com.upchina.activemq.pool;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.messaginghub.pooled.jms.pool.PooledConnection;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * Created by anjunli on  2024/2/4
 **/
public class MQPoolUtil {
    private static PooledConnection conn;

    public static void init(){
        String url="failover:(vm://localhost）";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(factory);
        pooledConnectionFactory.setMaxConnections(10);

    }
}
