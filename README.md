
## Context

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

We don`t want to create a `ConsumerFactory` bean and `ConcurrentKafkaListenerContainerFactory` for every @KafkaListener annoted method we have.

Cons:
* Boilerplate code for every topic we want to consume from
* Error-prone, easy to mistakes in the Java configuration (not using ContainerCustomizer) will lead to missing metrics and tracing

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

* Lot of code for "just" configuration 

## Our proposal

### Option 1:  


### Option 2: More control over the configuration of KafkaContainerListener using any Spring property source 

Some examples:
* More configuration options for `@KafkaListener` annotation to refer to property block, specified in `application.yml` or externalized configuration
* 

TODO check if possible

* ContainerPostProcessor
* ContainerCustomizer




