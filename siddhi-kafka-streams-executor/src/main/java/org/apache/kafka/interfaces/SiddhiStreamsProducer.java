package org.apache.kafka.interfaces;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.utils.SiddhiStreamsContract;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SiddhiStreamsProducer {

  public static void main(String[] args) {
    String topic = "siddhi-stream-topic9";

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "10.97.136.161:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    String key = "key";

    String streamId = "siddhiStream1";
    List<Object> firstData = Arrays.asList("Rectangle", 20.0, 20);
    SiddhiStreamsContract firstSiddhiStreamsContract = new SiddhiStreamsContract(streamId, firstData);

    List<Object> secondData = Arrays.asList("Square", 21.0, 21);
    SiddhiStreamsContract secondSiddhiStreamsContract = new SiddhiStreamsContract(streamId, secondData);

    byte[] firstValue = firstSiddhiStreamsContract.toString().getBytes();
    byte[] secondValue = secondSiddhiStreamsContract.toString().getBytes();

    ProducerRecord<String, byte[]> firstProducerRecord = new ProducerRecord<>(topic, key, firstValue);
    ProducerRecord<String, byte[]> secondProducerRecord = new ProducerRecord<>(topic, key, secondValue);

    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
    producer.send(firstProducerRecord);
    producer.send(secondProducerRecord);
    producer.close();
  }
}