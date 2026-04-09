package com.example.kafka4.spring.kafka.consumer.rest;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaContainerListenersController {

  private static final Logger log = LoggerFactory.getLogger(KafkaContainerListenersController.class);
  private final KafkaListenerEndpointRegistry registry;

  public KafkaContainerListenersController(KafkaListenerEndpointRegistry registry) {
    this.registry = registry;
  }

  @GetMapping
  public void print() {
    Collection<MessageListenerContainer> allListenerContainers = registry.getAllListenerContainers();
    for (MessageListenerContainer container : allListenerContainers) {
      log.info("Container: {}", container);
    }
  }


}
