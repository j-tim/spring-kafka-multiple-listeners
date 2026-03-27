package com.example.kafka4.springkafkaconsumer;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import com.example.kafka4.event.json.StockQuoteEventJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MultiKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(MultiKafkaListener.class);

    @KafkaListener(topics = "json-topic", containerFactory = "jsonKafkaListenerContainerFactory")
    public void onJsonEvent(StockQuoteEventJson event) {
        log.info("Received message from json-topic: {}", event);
    }

    @KafkaListener(topics = "avro-topic", containerFactory = "avroKafkaListenerContainerFactory")
    public void onAvroEvent(StockQuoteEventAvro event) {
        log.info("Received message from avro-topic: {}", event);
    }

}
