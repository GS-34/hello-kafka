package com.example.consumer;

import com.example.common.Const;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class SyncOffsetConsumer {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    KafkaConsumer<String, String> consumer = Const.defaultConsumer(properties);
    consumer.subscribe(Arrays.asList(Const.TOPIC_NAME));

    while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

      for (ConsumerRecord record : records) {
        log.info("record : {}", record);
        currentOffset.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1, null));
        consumer.commitSync(currentOffset);
      }

    }

  }

}
