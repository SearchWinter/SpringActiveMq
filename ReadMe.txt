
persistence=true

生产者
jmsTemplate.convertAndSend(LOG_QUEUE, i+msg);
1000rows 27s  25s  23s
5000rows 136s
10000rows 286s
11:28:47
消费者
1、final Message message = jmsTemplate.receive(LOG_QUEUE);
1000rows 3s 3s
5000rows 26s
10000rows 63s

2、@JmsListener
1000rows 25s  27s
5000rows 133s

concurrency = "5"
5000rows 133s

两个@JmsListener
5000rows 131s

*********************************************
persistence=false

生产者
jmsTemplate.convertAndSend(LOG_QUEUE, i+msg);
10000rows   274s


消费者
1、final Message message = jmsTemplate.receive(LOG_QUEUE);
10000rows   63s


结论：
    1、生产者速度和磁盘写入速度有关，磁盘占用很高;
    2、@JmsListener设置多个，修改concurrency参数都不能提升消费速度；
    3、生产者分多个destination，用多个JmsListener消费，最后耗时基本没有区别
    4、消息入消息队列，总的耗时和send()方法调用次数有关，通过汇总多条数据一次发送，减少调用次数，可以缩减总的耗时
    5、JmsListener 消费数据，Payload的值属性可以是String\List<String>
    @JmsListener(destination = LOG_QUEUE, concurrency = "1")
          public void listenerReceiveMsg4(@Payload List<String> msgs, @Headers MessageHeaders headers, Message message, Session session) {
              for (String msg : msgs) {
                  System.out.println("listenerReceiveMsg4: " + msg);
              }
          }
    6、使用同一个Producer一直发消息，手动管理事务，不能大幅度提升速度（除非数据发送后，统一commit一次） Boolean.TRUE；对应服务会打印更多的事务日志
     Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
     session.commit();
     session.rollback();
    7、设置prefetch值，默认1000，现象：日志重复打印ActiveMQMessageConsumer - on close, rollback duplicate:
      ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
      prefetchPolicy.setQueuePrefetch(10);
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
      factory.setPrefetchPolicy(prefetchPolicy);