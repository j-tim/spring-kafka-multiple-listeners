package com.example.kafka4.spring.kafka.consumer.configuration;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import com.example.kafka4.event.json.StockQuoteEventJson;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.micrometer.KafkaTemplateObservationConvention;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfiguration {

    private final KafkaProperties properties;

    public KafkaProducerConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    // ===================== Json ======================================================================================

    @Bean
    LoggingProducerListener<String, StockQuoteEventJson> jsonKafkaProducerListener() {
        return new LoggingProducerListener<>();
    }

    @Bean
    KafkaTemplate<String, StockQuoteEventJson> jsonKafkaTemplate(ProducerFactory<String, StockQuoteEventJson> kafkaProducerFactory,
                                                                 ProducerListener<String, StockQuoteEventJson> kafkaProducerListener,
                                                                 ObjectProvider<RecordMessageConverter> messageConverter,
                                                                 ObjectProvider<KafkaTemplateObservationConvention> observationConvention) {
        Map<String, Object> configurationOverrides = new HashMap<>();
        configurationOverrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        PropertyMapper map = PropertyMapper.get();
        KafkaTemplate<String, StockQuoteEventJson> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory, configurationOverrides);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        observationConvention.ifUnique(kafkaTemplate::setObservationConvention);
        map.from(kafkaProducerListener).to(kafkaTemplate::setProducerListener);
        map.from(this.properties.getTemplate().getDefaultTopic()).to(kafkaTemplate::setDefaultTopic);
        map.from(this.properties.getTemplate().getTransactionIdPrefix()).to(kafkaTemplate::setTransactionIdPrefix);
        map.from(this.properties.getTemplate().isObservationEnabled()).to(kafkaTemplate::setObservationEnabled);
        return kafkaTemplate;
    }

    // ===================== Avro ======================================================================================

    @Bean
    LoggingProducerListener<String, StockQuoteEventAvro> avroKafkaProducerListener() {
        return new LoggingProducerListener<>();
    }

    @Bean
    KafkaTemplate<String, StockQuoteEventAvro> avroKafkaTemplate(ProducerFactory<String, StockQuoteEventAvro> kafkaProducerFactory,
                                                                 ProducerListener<String, StockQuoteEventAvro> kafkaProducerListener,
                                                                 ObjectProvider<RecordMessageConverter> messageConverter,
                                                                 ObjectProvider<KafkaTemplateObservationConvention> observationConvention) {
        PropertyMapper map = PropertyMapper.get();
        KafkaTemplate<String, StockQuoteEventAvro> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        observationConvention.ifUnique(kafkaTemplate::setObservationConvention);
        map.from(kafkaProducerListener).to(kafkaTemplate::setProducerListener);
        map.from(this.properties.getTemplate().getDefaultTopic()).to(kafkaTemplate::setDefaultTopic);
        map.from(this.properties.getTemplate().getTransactionIdPrefix()).to(kafkaTemplate::setTransactionIdPrefix);
        map.from(this.properties.getTemplate().isObservationEnabled()).to(kafkaTemplate::setObservationEnabled);
        return kafkaTemplate;
    }

}
