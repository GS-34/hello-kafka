package com.example.producer;

import com.example.common.Const;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SimpleProducer {


  public static void main(String[] args) {

    KafkaProducer<String, String> producer = Const.defaultProducer(new Properties());

    int partitionNo = 0;
    String messageKey = "messageKey";
    String messageValue = "massageValue";
    ProducerRecord<String, String> record = null;

    //메세지
    record = new ProducerRecord<>(Const.TOPIC_NAME, messageValue);
    producer.send(record);

    //키 + 메시지
    record = new ProducerRecord<>(Const.TOPIC_NAME, messageKey, messageValue);
    producer.send(record);

    //파티션 + 키 + 메시지
    record = new ProducerRecord<>(Const.TOPIC_NAME, partitionNo, messageKey, messageValue);
    producer.send(record);


    producer.flush();
    producer.close();
  }

}
