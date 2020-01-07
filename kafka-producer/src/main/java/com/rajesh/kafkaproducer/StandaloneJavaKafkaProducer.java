package com.rajesh.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StandaloneJavaKafkaProducer {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		 props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
	        
		 Producer<String, SampleMessage> producer = new KafkaProducer<>(props);
		for (int ii = 0; ii < 100; ii++) {
		 	producer.send(new ProducerRecord<String, SampleMessage>(KafkaProducerApplication.TOPIC, Integer.toString(50), new SampleMessage(ii, "StandaloneJavaKafkaProducer-" + ii)));
		 }
		 producer.close();
		 
	}
}
