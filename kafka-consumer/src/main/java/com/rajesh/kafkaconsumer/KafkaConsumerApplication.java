package com.rajesh.kafkaconsumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@SpringBootApplication
public class KafkaConsumerApplication {
	
	public static final String TOPIC = "kafka_topic";

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

}

@Service
class KafkaConsumer {
	
	@KafkaListener(topics=KafkaConsumerApplication.TOPIC, groupId = "SampleMessage_consumer_group")
	public void consume(@Payload String message) {
		System.out.println("* Received " + message);
	}
}


@EnableKafka
@Configuration
class KafkaConfiguration {
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, SampleMessage> getKafkaListenerFactory() {
		
		Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "SampleMessage_consumer_group");
        config.put("value.deserializer", JsonDeserializer.class);
		config.put("key.deserializer", StringDeserializer.class);
        ConcurrentKafkaListenerContainerFactory<String, SampleMessage> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		listenerContainerFactory.setConsumerFactory(
				new DefaultKafkaConsumerFactory<>(
							config,
					      new StringDeserializer(), 
					      new JsonDeserializer<>(SampleMessage.class)));
		return listenerContainerFactory;
	}
}

class SampleMessage {

	private long id;
	private String message;
	
	public SampleMessage() {
	}
	
	public SampleMessage(long id, String message) {
		this.id = id;
		this.message = message;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "Message [id=" + id + ", message=" + message + "]";
	}

}

