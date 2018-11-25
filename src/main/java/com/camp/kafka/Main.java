package com.camp.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main
{
	
    public static void main(String args[])
    {
    	Properties props = new Properties();  
    	props.put("bootstrap.servers", "54.169.147.100:9092");  
    	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
    	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
    	props.put("acks", "all");  
    	props.put("retries", 1);  
    	String topic="test";
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);  
    	
    	while(true)
		{
    		System.out.println("input:");
    		Scanner sc = new Scanner(System.in);
			String read = sc.nextLine();
			producer.send(new ProducerRecord<String, String>(topic, read));
		}
    	  
    /*	producer.send(new ProducerRecord<String, String>(topic, "World"), new Callback() {  
    	     
    	    public void onCompletion(RecordMetadata metadata, Exception e) {  
    	        if (e != null) {  
    	            e.printStackTrace();  
    	        } else {  
    	            System.out.println(metadata.toString());//org.apache.kafka.clients.producer.RecordMetadata@1d89e2b5  
    	            System.out.println(metadata.offset());//1  
    	        }  
    	    }  
    	});  */
    	//producer.flush();  
    	
    	
    	//producer.close();  
    	
    	
    	//System.out.println("Complete");
    	
    }

}
