package com.example.consumer;

import com.example.common.Const;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class SimpleConsumer {

  public static void main(String[] args) {

    KafkaConsumer<String, String> consumer = Const.defaultConsumer(new Properties());
    consumer.subscribe(Arrays.asList(Const.TOPIC_NAME));

    while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

      for (ConsumerRecord record : records) {
        log.info("record : {}", record);
      }

    }

  }

}
