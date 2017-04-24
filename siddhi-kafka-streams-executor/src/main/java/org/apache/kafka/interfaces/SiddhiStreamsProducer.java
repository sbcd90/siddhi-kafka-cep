package org.apache.kafka.interfaces;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.utils.SiddhiStreamsContract;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class SiddhiStreamsProducer {
  private String topic;
  private String bootstrapServers;
  private String streamId;
  private KafkaProducer<String, byte[]> producer;


  public SiddhiStreamsProducer(String topic,
                               String bootstrapServers,
                               String streamId) {
    Objects.requireNonNull(topic, "Topic cannot be null");
    Objects.requireNonNull(bootstrapServers, "Bootstrap servers should point to valid kafka brokers location");
    Objects.requireNonNull(streamId, "Siddhi Stream Id cannot be null");

    this.topic = topic;
    this.bootstrapServers = bootstrapServers;
    this.streamId = streamId;

    this.createKafkaProducer();
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return properties;
  }

  private void createKafkaProducer() {
    producer = new KafkaProducer<>(getProperties());
  }

  private String getKey() {
    return "key";
  }

  private String getStreamId() {
    return streamId;
  }

  public void produce(Object... data) {
    Objects.requireNonNull(data, "Input data cannot be null");

    List<Object> dataList = Arrays.asList(data);
    SiddhiStreamsContract siddhiStreamsContract = new SiddhiStreamsContract(getStreamId(), dataList);
    byte[] dataBytes = siddhiStreamsContract.toString().getBytes();

    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, getKey(), dataBytes);

    if (Objects.nonNull(producer)) {
      producer.send(producerRecord);
    }
  }

  public void shutdown() {
    producer.close();
  }
}