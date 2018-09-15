package com.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class producer_with_key {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(producer_with_key.class);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
        prop.setProperty("key.serializer",StringSerializer.class.getName());
        prop.setProperty("value.serializer",StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);


        for(int i=0;i<10;i++) {
            String topic = "test_topic";
            String value = "hello World"+Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            logger.info("Key : "+key);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("Successfully sent");
                        logger.info("Received new metadata \n" +
                                "Tppic : " + recordMetadata.topic() + "\n" +
                                "Partition" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp() + "\n");

                    } else logger.error("Error while producing messages", e);
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}

