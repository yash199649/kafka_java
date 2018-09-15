package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producer {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
        prop.setProperty("key.serializer",StringSerializer.class.getName());
        prop.setProperty("value.serializer",StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        ProducerRecord<String,String> record= new ProducerRecord<String, String>("test_topic",
                "hello world");
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
