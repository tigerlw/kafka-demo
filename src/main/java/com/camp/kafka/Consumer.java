/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.camp.kafka;

import kafka.utils.ShutdownableThread;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer extends ShutdownableThread {
    private final org.apache.kafka.clients.consumer.Consumer<Integer, String> consumer;
    private final String topic;
    
    private String threadName;

    public Consumer(String topic,String name) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();  
		props.put("bootstrap.servers", "54.169.147.100:9092");  
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		props.setProperty("group.id", "0");  
		props.setProperty("enable.auto.commit", "true");  
		props.setProperty("auto.offset.reset", "latest");  
		//String topic="my-replicated-topic";
		
		consumer = new KafkaConsumer<Integer, String>(props);  
        //consumer = new KafkaConsumer<>(props);
        this.topic = "test";
        
        this.threadName = name;
    }

    @Override
    public void doWork() {
    	
    	
		consumer.subscribe(Collections.singletonList(topic));  
		  
		ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Thread:"+threadName+" Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset() + " partition :"+record.partition());
        }
        
        consumer.commitAsync();
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
}
