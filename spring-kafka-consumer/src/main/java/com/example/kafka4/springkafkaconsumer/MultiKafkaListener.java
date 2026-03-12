package com.example.kafka4.springkafkaconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MultiKafkaListener {

    @KafkaListener(topics = "topic1", groupId = "group1")
    public void listenToTopic1(String message) {
        System.out.println("Received message from topic1: " + message);
    }

    @KafkaListener(topics = "topic2", groupId = "group2")
    public void listenToTopic2(String message) {
        System.out.println("Received message from topic2: " + message);
    }

}
