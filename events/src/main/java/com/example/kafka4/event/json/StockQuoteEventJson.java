package com.example.kafka4.event.json;

import java.time.Instant;

public record StockQuoteEventJson(
        String symbol,
        String exchange,
        String tradeValue,
        String currency,
        String description,
        Instant tradeTime
) {
}
