package com.example.kafka4.spring.kafka.consumer.rest;

import com.example.kafka4.event.json.StockQuoteEventJson;
import com.example.kafka4.spring.kafka.consumer.producer.StockQuoteJsonProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/api/stock-quotes")
public class StockQuoteProducerController {

    private final StockQuoteJsonProducer producerService;

    public StockQuoteProducerController(StockQuoteJsonProducer producerService) {
        this.producerService = producerService;
    }

    /**
     * Send a stock quote event to the default topic.
     */
    @PostMapping
    public ResponseEntity<String> sendStockQuote(@RequestBody StockQuoteEventJson event) {
        producerService.sendStockQuote(event);
        return ResponseEntity.ok("Stock quote event sent successfully");
    }

    /**
     * Send a sample stock quote event for testing.
     */
    @PostMapping("/sample")
    public ResponseEntity<String> sendSampleStockQuote() {
        StockQuoteEventJson sampleEvent = new StockQuoteEventJson(
            "AAPL",
            "NASDAQ",
            "178.50",
            "USD",
            "Apple Inc. Common Stock",
            Instant.now()
        );
        producerService.sendStockQuote(sampleEvent);
        return ResponseEntity.ok("Sample stock quote event sent successfully");
    }
}

