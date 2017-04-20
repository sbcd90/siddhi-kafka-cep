package org.apache.kafka.interfaces;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.utils.SiddhiRuleContract;

import java.util.ArrayList;
import java.util.Properties;

public class SiddhiRuleProducer {

  public static void main(String[] args) {

    String topic = "siddhi-rule-topic9";

    Properties properties = new Properties();
    properties.put("bootstrap.servers", "10.97.136.161:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    String key = "key";

    ArrayList<String> definitions = new ArrayList<>();
    String definition = "define stream siddhiStream1 (symbol string, price double, volume long);";
    definitions.add(definition);

    String siddhiQuery = "@info(name = 'query1') from siddhiStream1[price < 20.0] " +
      "select symbol, price, volume insert into outputStream";

    byte[] value = new SiddhiRuleContract("siddhiStream1", definitions, siddhiQuery).toString().getBytes();

    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, value);

    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
    producer.send(producerRecord);
    producer.close();
  }
}