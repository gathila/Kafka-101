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
public class ProducerDemo {

    Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record1 = new ProducerRecord<>("first_topic", "Hello world");

        //async op
        producer.send(record1);

        //close it so that it will flush remaining messages before termination
        producer.close();
    }
}
