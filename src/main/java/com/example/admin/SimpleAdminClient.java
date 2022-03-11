package com.example.admin;

import com.example.common.Const;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

@Slf4j
public class SimpleAdminClient {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Const.BOOTSTRAP_SERVER);

    AdminClient admin = null;
    try {

      admin = AdminClient.create(properties);

      /* 클러스터 정보 조회 */
      for (Node node : admin.describeCluster().nodes().get()) {
        log.info("node : {}", node);

        /* 브로커별 정보 조회 */
        ConfigResource cr = new ConfigResource(Type.BROKER, node.idString());
        DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
        describeConfigs.all().get()
            .forEach((broker, config) ->

                config.entries().forEach(
                    configEntry -> log.info(configEntry.name() + " = " + configEntry.value())));
      }

      /* 토픽 정보 조회 */
      Map<String, TopicDescription> topicInfo = admin.describeTopics(
          Collections.singleton(Const.TOPIC_NAME)).all().get();

      log.info("topicInfo : {} ", topicInfo);
    } finally {
      if (Objects.nonNull(admin)) {
        admin.close();
      }
    }


  }

}
