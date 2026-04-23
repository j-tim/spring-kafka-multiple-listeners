package com.example.kafka4.spring.kafka.consumer.rest;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import com.example.kafka4.event.json.StockQuoteEventJson;
import com.example.kafka4.spring.kafka.consumer.producer.StockQuoteAvroProducer;
import com.example.kafka4.spring.kafka.consumer.producer.StockQuoteJsonProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/api/stock-quotes")
public class StockQuoteProducerController {

    private final StockQuoteJsonProducer stockQuoteJsonProducer;
    private final StockQuoteAvroProducer stockQuoteAvroProducer;

    public StockQuoteProducerController(StockQuoteJsonProducer stockQuoteJsonProducer, StockQuoteAvroProducer stockQuoteAvroProducer) {
        this.stockQuoteJsonProducer = stockQuoteJsonProducer;
        this.stockQuoteAvroProducer = stockQuoteAvroProducer;
    }

    /**
     * Send a sample stock quote event for testing.
     */
    @PostMapping("/sample")
    public ResponseEntity<String> sendSampleStockQuote() {
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

        return ResponseEntity.ok("Sample stock quote event sent successfully");
    }
}

