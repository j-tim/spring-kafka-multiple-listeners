package com.example.kafka4.springkafkaconsumer;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

//@Component
public class MultiKafkaListenerConfigUsingAnnotations {

    @KafkaListener(topics = "topic1", groupId = "group1")
    public void listenToTopic1(String message) {
        System.out.println("Received message from topic1: " + message);
    }

    @KafkaListener(topics = "topic2", groupId = "group2")
    public void listenToTopic2(String message) {
        System.out.println("Received message from topic2: " + message);
    }

    @KafkaListener(topics = "avro-topic", groupId = "group3", containerFactory = "my-avro-topic-factory", properties = {
            "value.deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
            "spring.json.trusted.packages=com.example.kafka.event.api.json",
            "kafka.payload.encryption.secret=no-secrets-in-code!"
    })
    public void consumeAvroEvent(StockQuoteEventAvro event) {
    }

}
