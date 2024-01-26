package com.upchina.activemq.embedbroker;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by anjunli on  2024/1/26
 **/
public class StartBroker {
    public static void main(String[] args) {
        try {
            // ActiveMQ 也支持在 vm 中通信基于嵌入式的 Broker
            BrokerService brokerService = new BrokerService();
            brokerService.setUseJmx(true);
            brokerService.addConnector("tcp://localhost:61616");

            PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();

            Path path = Paths.get("D://kaha");
            File dir = new File(path.toUri());
            if (!dir.exists()) {
                dir.mkdirs();
            }
            persistenceAdapter.setDirectory(dir);
            brokerService.setPersistenceAdapter(persistenceAdapter);
            brokerService.setPersistent(true);
            brokerService.start();

            //阻塞进程，让broker一直开启
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
