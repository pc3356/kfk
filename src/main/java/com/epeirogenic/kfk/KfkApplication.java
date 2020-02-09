package com.epeirogenic.kfk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KfkApplication implements CommandLineRunner {

	private final static Logger LOGGER = LoggerFactory.getLogger(KfkApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KfkApplication.class, args);
	}

	private final CountDownLatch latch;
	private final KafkaTemplate<String, String> template;

	KfkApplication(
			@Autowired final CountDownLatch countDownLatch,
			@Autowired final KafkaTemplate<String, String> kafkaTemplate
	) {
		this.latch = countDownLatch;
		this.template = kafkaTemplate;
	}

	@Override
	public void run(String... args) throws Exception {
		this.template.send("topic1", UUID.randomUUID().toString(), "foo1");
		this.template.send("topic1", UUID.randomUUID().toString(), "foo2");
		this.template.send("topic1", UUID.randomUUID().toString(), "foo3");
		latch.await(60, TimeUnit.SECONDS);
		LOGGER.info("All received");
	}
}
