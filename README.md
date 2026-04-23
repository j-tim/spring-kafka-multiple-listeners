# Multiple Spring Kafka consumer listeners in a single Spring Boot application  

## Context

In our company we have multiple teams consuming from multiple Kafka topics in a single Spring Boot application.
When consuming from multiple topics that require different configuration like:
* different deserialization format
* shared secrets for encryption

we want to avoid the boilerplate code of creating a `ConsumerFactory` bean and `KafkaListenerContainerFactory` bean for each topic.

This example project is input for [Spring Boot issue #50166](https://github.com/spring-projects/spring-boot/issues/50166)

## Requirements

### Microservice vs Mini service vs Structured monolith

* We see different teams with different deployment models
* We don't necessary embrace the idea that a single (micro) service consumes a single topic
* We see multiple teams requiring to consuming multiple Kafka topic in the same service
  * In many cases the service consumes events from multiple topics for the "same business domain"
  * In many the load is not high enough to justify the overhead of multiple services
* In many cases the services must consume from 7+ topics from the same Kafka cluster

### Option 1: Use `@KafkaListener` with configuration properties

```java
@KafkaListener(topics = "json-topic", groupId = "group3", properties = {
            "value.deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
            "spring.json.trusted.packages=com.example.kafka.event.api.json",
            "kafka.payload.encryption.secret=no-secrets-in-code!"
    })
    public void consumeJsonEvent(SomeJsonEvent event) {
    }
```

Cons:
* We don't want to have configuration in code for Kafka listeners.
  * Secret material should be not stored in code
    * will differ between environments
    * will differ between per topic
  * Different serialization formats for per topic (same over environments)
    * Avro
    * Json
    * Protobuf

* From the `@KafkaListener` we can't refer to properties in `application.yml` to externalize the configuration

### Option 2: Create `ConsumerFactory` bean and `KafkaListenerContainerFactory` Kafka topic to consume

We don`t want to create a `ConsumerFactory` bean and `ConcurrentKafkaListenerContainerFactory` for every @KafkaListener annotated method we have.

Cons:
* Boilerplate code for every topic we want to consume from like: [KafkaConsumerConfiguration.java](spring-kafka-consumer/src/main/java/com/example/kafka4/spring/kafka/consumer/configuration/KafkaConsumerConfiguration.java)
* Error-prone, easy to mistakes in the Java configuration (not using ContainerCustomizer) will lead to missing metrics and tracing
* Lot of code for "just" configuration

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(EnableKafka.class)
class KafkaAnnotationDrivenConfiguration {
    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }
}    
```

## Our proposal: Dynamically create KafkaListenerContainerFactory beans 

Dynamically create `KafkaListenerContainerFactory` beans for each consumer to be configured differently 
(like different deserialization format, shared secrets for encryption)  
based on the configuration in `application.yml`. 

Spring Kafka consumers is a map. Where each key will the name of the KafkaListenerContainerFactory bean. 

[See: application-proposal.yaml](spring-kafka-consumer/src/main/resources/application-proposal.yaml)

```yml
spring:
  kafka:
    # Propasal
    consumers:
      # Specific for each consumer.
      "my-avro-topic-consumer":
        consumer:
          group-id: my-avro-group
          value-deserializer: org.apache.kafka.common.serialization.AvroDeserializer
          auto-offset-reset: earliest
      "my-json-topic-consumer":
        consumer:
          group-id: my-json-group
          value-deserializer: org.apache.kafka.common.serialization.JsonDeserializer
          auto-offset-reset: latest
          bootstrap-servers: other-cluster:9092
        listener:
          observation-enabled: false
```

The name of this bean will be referenced in the `@KafkaListener` annotation to specify which container factory 
to use for each listener method.

```java
@Component
public class MultiKafkaListener {
    
  private static final Logger log = LoggerFactory.getLogger(MultiKafkaListener.class);

  @KafkaListener(topics = "avro-topic", containerFactory = "my-avro-topic-consumer")
  public void onAvroEvent(StockQuoteEventAvro event) {
    log.info("Received message from avro-topic: {}", event);
  }
  
  @KafkaListener(topics = "json-topic", containerFactory = "my-json-topic-consumer")
  public void onJsonEvent(StockQuoteEventJson event) {
    log.info("Received message from json-topic: {}", event);
  }
}
```

The developer doesn't need to create any `ConsumerFactory` bean or `KafkaListenerContainerFactory` bean for each consumer.
So this boilerplate code is removed and the Java configuration.

## Build and run the project

This project doesn't contain a solution for our proposal, but it contains the boilerplate code required to 
run the Spring Boot application with multiple Kafka consumers (Json and Avro serialization formats).

```bash
./mvnw clean install
docker compose up -d
./mvnw spring-boot:run -pl spring-kafka-consumer
```