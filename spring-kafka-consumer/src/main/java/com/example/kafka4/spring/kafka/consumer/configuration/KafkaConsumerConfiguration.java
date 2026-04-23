package com.example.kafka4.spring.kafka.consumer.configuration;

import com.example.kafka4.event.avro.StockQuoteEventAvro;
import com.example.kafka4.event.json.StockQuoteEventJson;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.*;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfiguration(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * We override the Spring Kafka bean from the autoconfiguration {@link KafkaAutoConfiguration#kafkaConsumerFactory(KafkaConnectionDetails, ObjectProvider)}
     */
    @Bean
    DefaultKafkaConsumerFactory<String, StockQuoteEventAvro> avroKafkaConsumerFactory(KafkaConnectionDetails connectionDetails,
                                                           ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers) {
        Map<String, Object> properties = this.kafkaProperties.buildConsumerProperties();
        applyKafkaConnectionDetailsForConsumer(properties, connectionDetails);
        DefaultKafkaConsumerFactory<String, StockQuoteEventAvro> factory = new DefaultKafkaConsumerFactory<>(properties);
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> avroKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, StockQuoteEventAvro> avroKafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        configurer.configure(factory, (ConsumerFactory) avroKafkaConsumerFactory);
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    // =================== Json ========================================================================================

    @Bean
    public DefaultKafkaConsumerFactory<String, StockQuoteEventJson> jsonKafkaConsumerFactory(KafkaConnectionDetails connectionDetails,
                                                                                             ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers) {
        Map<String, Object> properties = this.kafkaProperties.buildConsumerProperties();
        Map<String, Object> configurationOverrides = new HashMap<>(properties);
        configurationOverrides.put(ConsumerConfig.GROUP_ID_CONFIG, "multiple-consumer-beans-group-json");
        configurationOverrides.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configurationOverrides.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        configurationOverrides.put("spring.json.trusted.packages", "com.example.kafka4.event.json");

        applyKafkaConnectionDetailsForConsumer(properties, connectionDetails);
        DefaultKafkaConsumerFactory<String, StockQuoteEventJson> factory = new DefaultKafkaConsumerFactory<>(configurationOverrides);
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> jsonKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, StockQuoteEventJson> jsonKafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, (ConsumerFactory) jsonKafkaConsumerFactory);
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    // ======== Other methods need to copy because of visibility =======================================================

    /**
     * Copy of {@link KafkaAutoConfiguration#applyKafkaConnectionDetailsForConsumer(Map, KafkaConnectionDetails)}
     */
    private void applyKafkaConnectionDetailsForConsumer(Map<String, Object> properties,
                                                        KafkaConnectionDetails connectionDetails) {
        KafkaConnectionDetails.Configuration consumer = connectionDetails.getConsumer();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumer.getBootstrapServers());
        applySecurityProtocol(properties, connectionDetails.getSecurityProtocol());
        applySslBundle(properties, consumer.getSslBundle());
    }

    /**
     * Copy of {@link KafkaAutoConfiguration#applySecurityProtocol(Map, String)}
     */
    static void applySecurityProtocol(Map<String, Object> properties, @Nullable String securityProtocol) {
        if (StringUtils.hasLength(securityProtocol)) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }
    }

    /**
     * Copy of {@link KafkaAutoConfiguration#applySslBundle(Map, SslBundle)}
     */
    static void applySslBundle(Map<String, Object> properties, @Nullable SslBundle sslBundle) {
        if (sslBundle != null) {
            properties.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, SslBundleSslEngineFactory.class);
            properties.put(SslBundle.class.getName(), sslBundle);
        }
    }
}
