package org.apache.kafka.interfaces;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.utils.JsonDeserializer;
import org.apache.kafka.utils.SiddhiRuleContract;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

public class SiddhiRuleConsumer {

  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "10.97.136.161:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList("siddhi-sink-topic16"));

    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(100);
      for (ConsumerRecord<String, byte[]> record: records) {
        SiddhiRuleContract ruleContract =
          new JsonDeserializer<>(SiddhiRuleContract.class).deserialize(record.key(), record.value());

        System.out.println("Start record");
        System.out.println(getRule(ruleContract));
        System.out.println("End record");
      }
    }
  }

  private static String getRule(SiddhiRuleContract siddhiRuleContract) {
    if (Objects.nonNull(siddhiRuleContract.getDefinitions())) {
      StringBuilder definitionsStr = new StringBuilder(String.join("\n", siddhiRuleContract.getDefinitions()));

      if (Objects.nonNull(siddhiRuleContract.getSiddhiQuery())) {
        StringBuilder queryStr = new StringBuilder(siddhiRuleContract.getSiddhiQuery());
        definitionsStr  = definitionsStr.append("\n").append(queryStr);
      }

      return definitionsStr.toString();
    }
    throw new RuntimeException("The Siddhi Cep Rule cannot be created");
  }
}