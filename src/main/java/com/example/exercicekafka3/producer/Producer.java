package com.example.exercicekafka3.producer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import com.example.exercicekafka3.model.DataFile;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class Producer {
	Integer i =0;
	
	public void sendRecord() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// configure serializers classes:
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
		// configure schema repository server
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
	
		
		// create the producer expecting avro schema:
		KafkaProducer<Integer, DataFile> avroProducer = new KafkaProducer<>(props);
	
		
		//read the file input:
		
		String line;
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader("data/info.csv"));
		
		try {
			while((line = in.readLine())!=null) {
				System.out.println(line);
				// create the Avro objets for key and value:
				String [] current_record = line.split(",");
				DataFile dataFile = new DataFile();
				dataFile.setSerialNumber(current_record[0]);
				dataFile.setActivityDate(current_record[1]);
				dataFile.setFacilityName(current_record[2]);
				dataFile.setScore(current_record[3]);
				dataFile.setGrade(current_record[4]);
				dataFile.setServiceCode(current_record[5]);
				dataFile.setServiceDescription(current_record[6]);
				dataFile.setEmployeeId(current_record[7]);
				dataFile.setFacilityAddress(current_record[8]);
				dataFile.setFacilityCity(current_record[9]);
				dataFile.setFacilityId(current_record[10]);
				dataFile.setFacilityState(current_record[11]);
				dataFile.setFacilityZip(current_record[12]);
				dataFile.setOwnerId(current_record[13]);
				dataFile.setOwnerName(current_record[14]);
				dataFile.setPeDescription(current_record[15]);
				dataFile.setProgramElementPe(current_record[16]);
				dataFile.setProgramName(current_record[17]);
				dataFile.setProgramStatus(current_record[18]);
				dataFile.setRecordId(current_record[19]);

				System.out.println("avant avro record");
				//create an avro record for each line and send it to the topic:
				ProducerRecord<Integer, DataFile> record = new ProducerRecord<>("my_avro_topic", + i, dataFile);
				avroProducer.send(record);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
	
	}
	
	
}
