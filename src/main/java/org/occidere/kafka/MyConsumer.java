package org.occidere.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MyConsumer {
	private final KafkaConsumer<String, String> CONSUMER;
	private final String SERVERS;
	public MyConsumer(String servers) {
		this.SERVERS = servers;
		this.CONSUMER = getKafkaConsumer();
	}
	
	private KafkaConsumer<String, String> getKafkaConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		return new KafkaConsumer<>(props);
	}
	
	public void run(final String TOPIC) {
		CONSUMER.subscribe(Arrays.asList(TOPIC));
		
		ConsumerRecords<String, String> records = CONSUMER.poll(1000);
		for(ConsumerRecord<String, String> record : records) {
			String topic = record.topic();
			if(topic.equals(TOPIC)) {
				System.out.println(record);
			}
		}
	}
	
	public void close() {
		CONSUMER.close();
	}
}