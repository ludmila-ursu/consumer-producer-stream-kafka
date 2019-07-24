package com.example.exercicekafka3.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;




public class Consumer {

	TopicPartition topicPartition = new TopicPartition("my_topic", 0);

	public void consumerSubscription() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupIdExample");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {

			consumer.subscribe(Arrays.asList("my_topic"));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(),
							record.value());
					
				consumer.seek(topicPartition, 5);
					
				}
			}
			
		}
	}
}	
