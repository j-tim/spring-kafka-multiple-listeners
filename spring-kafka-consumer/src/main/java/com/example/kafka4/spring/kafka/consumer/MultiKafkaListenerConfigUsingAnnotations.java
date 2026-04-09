package com.example.kafka4.spring.kafka.consumer;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import com.example.kafka4.event.json.StockQuoteEventJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

//@Component
public class MultiKafkaListenerConfigUsingAnnotations {

    private static final Logger log = LoggerFactory.getLogger(MultiKafkaListenerConfigUsingAnnotations.class);

    @KafkaListener(topics = "topic-avro", groupId = "group1")
    public void listenToTopic1(StockQuoteEventAvro eventAvro) {
        log.info("Received message from `topic-avro`: {}", eventAvro);
    }

    @KafkaListener(topics = "topic-json", groupId = "group3", containerFactory = "my-avro-topic-factory", properties = {
            "value.deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
            "spring.json.trusted.packages=com.example.kafka4.event.json",
            "kafka.payload.encryption.secret=no-secrets-in-code!"
    })
    public void consumeJsonEvent(StockQuoteEventJson event) {
        log.info("Received message from `topic-json`: {}", event);
    }

}
