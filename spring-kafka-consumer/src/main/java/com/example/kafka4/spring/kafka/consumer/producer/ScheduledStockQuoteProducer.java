package com.example.kafka4.spring.kafka.consumer.producer;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import com.example.kafka4.event.json.StockQuoteEventJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@EnableScheduling
@Component
public class ScheduledStockQuoteProducer {

    private static final Logger log = LoggerFactory.getLogger(ScheduledStockQuoteProducer.class);
    private final StockQuoteJsonProducer stockQuoteJsonProducer;
    private final StockQuoteAvroProducer stockQuoteAvroProducer;

    public ScheduledStockQuoteProducer(StockQuoteJsonProducer stockQuoteJsonProducer, StockQuoteAvroProducer stockQuoteAvroProducer) {
        this.stockQuoteJsonProducer = stockQuoteJsonProducer;
        this.stockQuoteAvroProducer = stockQuoteAvroProducer;
    }

    @Scheduled(fixedRate = 10000)
    public void sendStockQuoteEvent() {
        log.info("Scheduling stock quote event...");
        String symbol = "AAPL";
        String exchange = "NASDAQ";
        String tradeValue = "178.50";
        String currency = "USD";
        String description = "Apple Inc. Common Stock";
        Instant tradeTime = Instant.now();

        stockQuoteJsonProducer.sendStockQuote(new StockQuoteEventJson(
                symbol,
                exchange,
                tradeValue,
                currency,
                description,
                tradeTime
        ));

        stockQuoteAvroProducer.sendStockQuote(new StockQuoteEventAvro(
                symbol,
                exchange,
                tradeValue,
                currency,
                description,
                tradeTime));
    }
}
