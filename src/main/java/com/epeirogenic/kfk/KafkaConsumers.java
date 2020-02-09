package com.epeirogenic.kfk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumers {

    private final CountDownLatch countDownLatch;

    KafkaConsumers(@Autowired final CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumers.class);

    @KafkaListener(topics = "#{topic1.name()}", autoStartup = "true")
    public String listen(final String message) {
        LOGGER.info("Message: " + message);
        countDownLatch.countDown();
        return message;
    }
}
