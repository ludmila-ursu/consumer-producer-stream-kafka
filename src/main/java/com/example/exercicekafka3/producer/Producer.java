package com.example.exercicekafka3.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class Producer {

	private String k1 = "my_key1";
	private String k2 = "my_key2";
	private String v1 = "my_value1";
	private String v2 = "my_value2";

	public void sendRecord() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "true");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("my_topic", k1, v1);
		ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("my_topic", k2, v2);
		
		
		producer.initTransactions();
		try {	
		producer.beginTransaction();
			producer.send(record1);
			producer.send(record2);
			producer.commitTransaction();
		}
		catch(ProducerFencedException e){
			producer.close();
		}
		catch(KafkaException e) {
			producer.abortTransaction();
		}
//		producer.send(record, (recordMetadata, e) -> {
//			if (e != null) {
//			e.printStackTrace();
//			} else {
//			System.out.println("Message String = " + record.value() + ", Offset = " + recordMetadata
//			.offset());
//			}
//			});
		producer.close();
	}
}

