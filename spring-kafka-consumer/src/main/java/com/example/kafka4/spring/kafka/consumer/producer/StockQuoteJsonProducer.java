package com.example.kafka4.spring.kafka.consumer.producer;

import com.example.kafka4.event.json.StockQuoteEventJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class StockQuoteJsonProducer {

    private static final Logger log = LoggerFactory.getLogger(StockQuoteJsonProducer.class);
    private static final String TOPIC = "json-topic";

    private final KafkaTemplate<String, StockQuoteEventJson> kafkaTemplate;

    public StockQuoteJsonProducer(KafkaTemplate<String, StockQuoteEventJson> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, StockQuoteEventJson>> sendStockQuote(StockQuoteEventJson event) {
        log.info("Sending stock quote event: {}", event);
        
        CompletableFuture<SendResult<String, StockQuoteEventJson>> future = 
            kafkaTemplate.send(TOPIC, event.symbol(), event);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message=[{}] with offset=[{}]", 
                    event, result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message=[{}] due to: {}", 
                    event, ex.getMessage());
            }
        });
        
        return future;
    }
}

