package com.gathi.demos.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * These are the steps which required to create a producer
 * create producer properties
 * create the producer
 * send the data
 * flush and close the producer
 */
public class ProducerDemoStickyPartitionerDemo {

    Logger logger = LoggerFactory.getLogger(ProducerDemoStickyPartitionerDemo.class.getName());

    public static void main(String[] args) {


    }

    public void producer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<10; i++) {
            for (int j=0; j<100; j++) {
                ProducerRecord<String, String> record1 = new ProducerRecord<>("first_topic", "Hello world " + i);

                //async op
                producer.send(record1, (recordMetadata, e) -> {
                    StringBuilder logBuilder = new StringBuilder();
                    logBuilder.append("topic name: " + recordMetadata.topic())
                            .append("\nPartition: " + recordMetadata.partition())
                            .toString();
                    logger.info(logBuilder.toString());
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //close it so that it will flush remaining messages before termination
        producer.close();
    }
}
