package com.example.kafka4.spring.kafka.consumer.producer;

import com.example.kafka4.event.json.StockQuoteEventJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@EnableScheduling
@Component
public class ScheduledStockQuoteJsonProducer {

    private static final Logger log = LoggerFactory.getLogger(ScheduledStockQuoteJsonProducer.class);
    private final StockQuoteJsonProducer stockQuoteJsonProducer;

    public ScheduledStockQuoteJsonProducer(StockQuoteJsonProducer stockQuoteJsonProducer) {
        this.stockQuoteJsonProducer = stockQuoteJsonProducer;
    }

    @Scheduled(fixedRate = 1000)
    public void sendStockQuoteEvent() {
        log.info("Scheduling stock quote event...");
        stockQuoteJsonProducer.sendStockQuote(new StockQuoteEventJson(
            "AAPL",
            "NASDAQ",
            "178.50",
            "USD",
            "Apple Inc. Common Stock",
                Instant.now()
        ));
    }
}
