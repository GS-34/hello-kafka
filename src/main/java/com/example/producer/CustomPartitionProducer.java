package com.example.producer;

import com.example.common.Const;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitionProducer {

  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
    KafkaProducer<String, String> producer = Const.defaultProducer(properties);

    ProducerRecord<String, String> record = new ProducerRecord<>(Const.TOPIC_NAME, "Pangyo", "messageValue");
    producer.send(record);

    producer.flush();
    producer.close();
  }

}

class CustomPartitioner implements Partitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {

    if (Objects.isNull(keyBytes)) {
      throw new InvalidRecordException("Need message key");
    }

    //키값이 "Pangyo" 라면, 0번 파티션 강제지정
    if (key.equals("Pangyo")) {
      return 0;
    }

    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    return Utils.toPositive(Utils.murmur2(keyBytes)) & numPartitions;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
