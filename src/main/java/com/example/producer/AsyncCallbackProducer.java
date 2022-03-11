package com.example.producer;

import com.example.common.Const;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class AsyncCallbackProducer {

  public static void main(String[] args) {

    KafkaProducer<String, String> producer = Const.defaultProducer(new Properties());

    ProducerRecord<String, String> record = new ProducerRecord<>(Const.TOPIC_NAME, "messageValue");
    producer.send(record, (metadata, exception) -> {
      if (Objects.nonNull(exception)) {
        log.error("Exception : ", exception);
      } else {
        log.info(metadata.toString());
      }
    });

    producer.flush();
    producer.close();

  }

}
