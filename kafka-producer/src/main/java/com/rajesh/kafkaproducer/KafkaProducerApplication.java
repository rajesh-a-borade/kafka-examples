package com.rajesh.kafkaproducer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

@SpringBootApplication
public class KafkaProducerApplication {
	
	public static final String TOPIC = "kafka_topic";
	public static final AtomicLong COUNTER = new AtomicLong(1L);

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}
}

@RestController
@RequestMapping("/kafka")
class KakfkaController {
	
	@Autowired
	private KafkaTemplate<String, SampleMessage> KafkaTemplate;
	
	@SuppressWarnings("unchecked")
	@GetMapping("/produce")
	public String get() {
		SampleMessage message = new SampleMessage(KafkaProducerApplication.COUNTER.getAndIncrement(), UUID.randomUUID().toString());
		ListenableFuture<SendResult<String, SampleMessage>>  res = KafkaTemplate.send(KafkaProducerApplication.TOPIC, message);
		res.addCallback(new SuccessCallback() {
			@Override
			public void onSuccess(Object arg0) {
				System.out.println("*** onSuccess");
			}
			
		}, new FailureCallback() {
			@Override
			public void onFailure(Throwable arg0) {
				System.out.println("*** onFailed");
			}
		});
		System.out.println("* Sent " + message);
		return "* Sent " + message;
	}
}

@Configuration
class KafkaConfiguration {
	
	public ProducerFactory<String, SampleMessage> getProducerFactory() {
		Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
	}
	
	@Bean
	public KafkaTemplate<String, SampleMessage> getKafkaTemplate() {
		return new KafkaTemplate<>(getProducerFactory());
	}
}

@Configuration
class KafkaTopicConfig {
     
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return new KafkaAdmin(configs);
    }
     
    @Bean
    public NewTopic topic1() {
         return new NewTopic(KafkaProducerApplication.TOPIC, 1, (short) 1);
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

