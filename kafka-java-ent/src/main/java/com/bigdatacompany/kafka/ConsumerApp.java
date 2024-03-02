package com.bigdatacompany.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.shaded.com.google.protobuf.Duration;

public class ConsumerApp {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,new StringDeserializer().getClass().getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,new StringDeserializer().getClass().getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdataTeam1");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG,"exam1");

        kafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(config);
        kafkaConsumer.subscribe(Arrays.asList("search"));
        while(true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ZERO);

            for(ConsumerRecord<String,String> rec: records)
                System.out.println(rec.value());
        }
    }
}
