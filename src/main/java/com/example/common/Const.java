package com.example.common;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Const {

  public static final String TOPIC_NAME = "simple-test";

  public static final String BOOTSTRAP_SERVER = "localhost:9092";

  public static final String GROUP_ID = "test-group";

  public static final int PARTITION_NO = 0;

  public static KafkaProducer<String, String> defaultProducer(Properties properties) {
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Const.BOOTSTRAP_SERVER);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    return new KafkaProducer<>(properties);
  }

  public static KafkaConsumer<String, String> defaultConsumer(Properties properties) {
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Const.BOOTSTRAP_SERVER);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, Const.GROUP_ID);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    return new KafkaConsumer<>(properties);
  }


}