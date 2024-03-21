package com.unifonic.consumer;

import com.savoirtech.logging.slf4j.json.LoggerFactory;
import com.savoirtech.logging.slf4j.json.logger.Logger;
import java.util.HashMap;
import java.util.Map;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;


/**
 * @author Hassaballah created on 20-Mar-2024
 */
@Configuration
public class RabbitMQConfig {

  private final static Logger logger = LoggerFactory.getLogger(RabbitMQConfig.class);

  @Value("${spring.rabbitmq.host}")
  private String host;

  @Value("${spring.rabbitmq.port}")
  private int port;

  @Value("${spring.rabbitmq.username}")
  private String username;

  @Value("${spring.rabbitmq.password}")
  private String password;

  @Value("${rabbitmq.queue}")
  private String queue;

  @Bean
  public ConnectionFactory connectionFactory() {
    logger.info().message("Internal connection:" + host + ", port:" + port + ", usr:"
                          + username + " , pass:" + password + " , queue: " + queue)
        .field("host", host)
        .field("port", port)
        .field("queue", queue)
        .field("username", username)
        .log();

    CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
    connectionFactory.setUsername(username);
    connectionFactory.setPassword(password);
    connectionFactory.setPublisherReturns(true);

    return connectionFactory;
  }

  @Bean
  public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory() {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory());
    return factory;
  }

  // @Bean
  // public Queue queue() {
  //   Map<String, Object> arguments = new HashMap<>();
  //   arguments.put("x-queue-type", "quorum");
  //
  //   return new Queue(queue, true, false, false, arguments);
  // }

}
