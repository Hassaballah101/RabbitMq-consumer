package com.unifonic.consumer;

import com.savoirtech.logging.slf4j.json.LoggerFactory;
import com.savoirtech.logging.slf4j.json.logger.Logger;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Hassaballah created on 20-Mar-2024
 */
@Component
public class MessageConsumer {

  private final static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

  private int counter = 0;

  @Autowired
  private RabbitTemplate rabbitTemplate;

  @PostConstruct
  public void init() {
    logger.info().message("Queue listener is ready")
        .field("counter", counter)
        .log();
  }

  @RabbitListener(queues = "myqueue")
  public void consumeMessage(Message message) {

    try {

      if (message != null) {
        logger.info().message("Received message from queue:" + counter)
            .field("message", new String(message.getBody(), StandardCharsets.UTF_8))
            .field("counter", counter)
            .log();
        counter++;
      }

      Thread.sleep(500); // Wait for half a sec
    } catch (Exception exception) {
      logger.error().message("Exception happened while receiving message from queue")
          .stack()
          .exception("exception", exception)
          .log();
      counter--;
    }

  }
}
