package org.apache.kafka.interfaces;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.utils.JsonDeserializer;
import org.apache.kafka.utils.SiddhiStreamsContract;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

public class SiddhiStreamsConsumer {
  private String topic;
  private String bootstrapServers;
  private KafkaConsumer<String, byte[]> consumer;

  /**
   * Constructor
   * @param topic kafka topic to produce
   * @param bootstrapServers kafka broker coordinates
   */
  public SiddhiStreamsConsumer(String topic,
                               String bootstrapServers) {
    Objects.requireNonNull(topic, "Topic cannot be null");
    Objects.requireNonNull(bootstrapServers, "Bootstrap servers should point to valid kafka brokers location");

    this.topic = topic;
    this.bootstrapServers = bootstrapServers;

    this.createKafkaConsumer();
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return properties;
  }

  private void createKafkaConsumer() {
    consumer = new KafkaConsumer<>(getProperties());
    consumer.subscribe(Arrays.asList(topic));
  }

  /**
   * Api to consume events
   * @param interval interval to poll events
   * @param consumerFunction consumer Function to retrieve events
   * @throws IOException
   */
  public void consume(int interval, Consumer<Object[]> consumerFunction) throws IOException {
    List<Object[]> dataList = new ArrayList<>();

    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(interval);
      for (ConsumerRecord<String, byte[]> record: records) {
        ObjectMapper mapper = new ObjectMapper();

        SiddhiStreamsContract siddhiStreamsContract =
          new JsonDeserializer<>(SiddhiStreamsContract.class).deserialize(record.key(),
            mapper.readTree(record.value()).asText().getBytes());

        consumerFunction.accept(siddhiStreamsContract.getData().toArray());
      }
    }
  }
}