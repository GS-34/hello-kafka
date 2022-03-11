package com.example.consumer;

import com.example.common.Const;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class AsyncOffsetConsumer {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    KafkaConsumer<String, String> consumer = Const.defaultConsumer(properties);
    consumer.subscribe(Arrays.asList(Const.TOPIC_NAME));

    while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

      for (ConsumerRecord record : records) {
        log.info("record : {}", record);
      }

      consumer.commitAsync((offsets, exception) -> {
        if (Objects.nonNull(exception)) {
          log.error("Commit failed");
        } else {
          log.info("Commit succeeded");
        }
      });

    }

  }

}
