package org.apache.kafka.interfaces;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.utils.SiddhiRuleContract;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class SiddhiRuleProducer {

  private String topic;
  private String bootstrapServers;
  private KafkaProducer<String, byte[]> producer;

  public SiddhiRuleProducer(String topic, String bootstrapServers) {
    Objects.requireNonNull(topic, "Topic cannot be null");
    Objects.requireNonNull(bootstrapServers, "Bootstrap servers should point to valid kafka brokers location");
    this.topic = topic;
    this.bootstrapServers = bootstrapServers;

    this.createProducer();
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return properties;
  }

  private void createProducer() {
    producer = new KafkaProducer<>(getProperties());
  }

  private String getKey() {
    return "key";
  }

  public void createRule(String streamId, ArrayList<String> definitions, String siddhiQuery) {
    Objects.requireNonNull(streamId, "Stream Id cannot be null");
    Objects.requireNonNull(definitions, "Siddhi Rule Definitions cannot be null");
    Objects.requireNonNull(siddhiQuery, "Siddhi Rule Query cannot be null");

    byte[] rule = new SiddhiRuleContract(streamId, definitions, siddhiQuery).toString().getBytes();

    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, getKey(), rule);
    producer.send(producerRecord);
  }

  public void shutdown() {
    producer.close();
  }
}