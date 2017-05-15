package com.ucloudlink.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//mport org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerMain
{
	public static void main(String args[])
	{
		Properties props = new Properties();  
		//props.put("bootstrap.servers", "54.169.147.100:9092");  
		props.put("zookeeper.connect", "54.169.147.100:2181");  
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		props.setProperty("group.id", "0");  
		props.setProperty("enable.auto.commit", "true");  
		//props.setProperty("auto.offset.reset", "latest");  
		String topic="test";
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        
        kafka.javaapi.consumer.ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (final KafkaStream stream : streams) {
            executor.submit(new Runnable() {
                public void run() {
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    while (it.hasNext()) {
                        MessageAndMetadata<byte[], byte[]> mm = it.next();
                        System.out.println(String.format("partition = %s, offset = %d, key = %s, value = %s", mm.partition(), mm.offset(), mm.key(), new String(mm.message())));
                    }
                }
            });
        }
		
	/*	Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);  
		consumer.subscribe(Collections.singletonList(topic));  
		  
		for (int i = 0; i < 2000; i++) {  
		    ConsumerRecords<String, String> records = consumer.poll(1000);  
		    System.out.println(records.count());  
		    for (ConsumerRecord<String, String> record : records) {  
		        System.out.println(record);  
		        //consumer.seekToBeginning(new TopicPartition(record.topic(), record.partition()));  
		    }  
		}  */
	}
	

}
