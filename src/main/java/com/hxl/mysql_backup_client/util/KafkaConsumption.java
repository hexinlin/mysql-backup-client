package com.hxl.mysql_backup_client.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;



public class KafkaConsumption {
	
	public  KafkaConsumer<String, String> consumer = null;
	public  String TOPIC = null;
	private final static Logger _logger = Logger.getLogger(KafkaConsumption.class);

	
	public KafkaConsumption(){
		Properties properties = new Properties();
        InputStream inputStream = KafkaConsumption.class.getClassLoader().getResourceAsStream("kafka.properties");
        try {
			properties.load(inputStream);
			TOPIC = properties.getProperty("binlong.topic");
			properties.remove("binlong.topic");
		} catch (IOException e) {
			e.printStackTrace();
			_logger.error(e);
		}
        consumer = new KafkaConsumer<String, String>(properties);
   
	}
	
	
	public static void getEntries() {
		
		/* TopicPartition p = new TopicPartition(_TOPIC,0);
		 _consumer.assign(Arrays.asList(p));
		 _consumer.seek(p, 0);*/
		
	}
}
