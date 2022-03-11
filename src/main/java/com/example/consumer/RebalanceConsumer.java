package com.example.consumer;

import com.example.common.Const;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class RebalanceConsumer {

  private static KafkaConsumer<String, String> consumer;
  private static Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumer = Const.defaultConsumer(properties);
    consumer.subscribe(Arrays.asList(Const.TOPIC_NAME), new RebalanceListener());

    while (true) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

      for (ConsumerRecord record : records) {

        log.info("record : {}", record);

        currentOffset.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1, null));

        consumer.commitSync(currentOffset);
      }
    }
  }

  @Slf4j
  private static class RebalanceListener implements ConsumerRebalanceListener {

    /* 리벨런스가 시작되기 직전에 호출 */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      log.warn("Partitions are assigned");
    }

    /* 리벨런스가 끝난 뒤에 파티션 할당이 완료되면 호출 */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      log.warn("Partitions are revoked");
      //마지막으로 처리 완료한 레코드를 기준으로 커밋, 데이터처리 중복을 방지 할 수 있다.
      consumer.commitSync(currentOffset);
    }
  }
}


