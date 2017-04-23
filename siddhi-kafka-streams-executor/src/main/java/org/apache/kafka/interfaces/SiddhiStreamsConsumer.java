package org.apache.kafka.interfaces;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.utils.JsonDeserializer;
import org.apache.kafka.utils.SiddhiStreamsContract;

import java.util.Arrays;
import java.util.Properties;

public class SiddhiStreamsConsumer {

  public static void main(String[] args) throws Exception {

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "10.97.136.161:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList("siddhi-stream-sink-topic16"));

    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(100);
      for (ConsumerRecord<String, byte[]> record: records) {
        ObjectMapper mapper = new ObjectMapper();


        SiddhiStreamsContract streamsContract =
          new JsonDeserializer<>(SiddhiStreamsContract.class).deserialize(record.key(),
            mapper.readTree(record.value()).asText().getBytes());
        System.out.println(streamsContract.getData());
      }
    }
  }
}