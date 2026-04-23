package com.example.kafka4.spring.kafka.consumer.producer;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class StockQuoteAvroProducer {

    private static final Logger log = LoggerFactory.getLogger(StockQuoteAvroProducer.class);
    private static final String TOPIC = "avro-topic";

    private final KafkaTemplate<String, StockQuoteEventAvro> kafkaTemplate;

    public StockQuoteAvroProducer(KafkaTemplate<String, StockQuoteEventAvro> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, StockQuoteEventAvro>> sendStockQuote(StockQuoteEventAvro event) {
        log.info("Sending stock quote event: {}", event);

        CompletableFuture<SendResult<String, StockQuoteEventAvro>> future =
                kafkaTemplate.send(TOPIC, event.getSymbol(), event);

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
