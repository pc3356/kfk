package com.epeirogenic.kfk;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

@Service
public final class RunService {

    private final static Logger LOGGER = LoggerFactory.getLogger(RunService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConsumerFactory<String, String> consumerFactory;
    private final NewTopic topic;

    private Consumer<String, String> consumer;

    public RunService(
            final @Autowired KafkaTemplate<String, String> kafkaTemplate,
            final @Autowired ConsumerFactory<String, String> consumerFactory,
            final @Autowired NewTopic topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.consumerFactory = consumerFactory;
        this.topic = topic;

        initConsumer();


    }

    private void initConsumer() {
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singletonList(topic.name()));
    }

    public String run() {

        final var keyUUID = UUID.randomUUID();
        final var key = keyUUID.toString();

        final var valueUUID = UUID.randomUUID();
        final var value = valueUUID.toString();

        for(var i = 0; i < 10; i++) {
            kafkaTemplate.send(topic.name(), key, value);
            LOGGER.info("Message sent ({}/10): {} : {}", (i + 1), key, value);
        }

        final var res = consumer.poll(Duration.ofMillis(500));

        final var sb = new StringBuilder();
        res.forEach(m -> sb.append(m.key()).append(" :: ").append(m.value()).append('\n'));

        return sb.toString();
    }
}
