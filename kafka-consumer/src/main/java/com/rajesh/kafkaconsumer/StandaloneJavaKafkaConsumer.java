package com.rajesh.kafkaconsumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StandaloneJavaKafkaConsumer {

	public static void main(String[] args) {
		
		Properties props = new Properties();
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("group.id", "SampleMessage_consumer_group");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("value.deserializer", StringDeserializer.class);
	     props.put("key.deserializer", StringDeserializer.class);
	     org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
	     kafkaConsumer.subscribe(Arrays.asList(KafkaConsumerApplication.TOPIC));
	     
	     while (true) {
	         ConsumerRecords<String, String> records = kafkaConsumer.poll(5000);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	         
	     }
	     // kafkaConsumer.close();
	}
}
