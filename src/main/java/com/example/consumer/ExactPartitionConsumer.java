package com.example.consumer;

import com.example.common.Const;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class ExactPartitionConsumer {

  public static void main(String[] args) {

    KafkaConsumer<String, String> consumer = Const.defaultConsumer(new Properties());

    consumer.assign(//특정 파티션 컨슈밍
        Collections.singleton(new TopicPartition(Const.TOPIC_NAME, Const.PARTITION_NO)));

    consumer//컨슈머에 할당 된 파티션 정보
        .assignment()
        .stream()
        .forEach(t -> log.info("topic : {}, partition : {}", t.topic(), t.partition()));

    while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

      for (ConsumerRecord record : records) {
        log.info("record : {}", record);
      }

    }

  }

}
