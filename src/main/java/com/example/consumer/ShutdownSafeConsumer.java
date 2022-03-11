package com.example.consumer;

import com.example.common.Const;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
public class ShutdownSafeConsumer {

  public static void main(String[] args) {

    KafkaConsumer<String, String> consumer = Const.defaultConsumer(new Properties());
    consumer.subscribe(Arrays.asList(Const.TOPIC_NAME));

    Thread mainThread = Thread.currentThread();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("consumer wakeup!!");
      consumer.wakeup();
      while (mainThread.isAlive()){
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }));

    try {
      while (true) {

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        for (ConsumerRecord record : records) {
          log.info("record : {}", record);
        }

      }
    } catch (WakeupException e) {
      /* wakeup() 이 호출 된 이후, poll() 이 호출 되면 발생 */
      log.warn("Wakeup consumer");
    } finally {
      consumer.close();
    }

    System.out.println("EXIT");


  }

}
