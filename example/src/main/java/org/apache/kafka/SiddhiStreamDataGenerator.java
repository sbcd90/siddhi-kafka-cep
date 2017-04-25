package org.apache.kafka;

import org.apache.kafka.interfaces.SiddhiStreamsProducer;

import java.util.Arrays;
import java.util.List;

public class SiddhiStreamDataGenerator {
  private static final String topic = "siddhi-stream-topic17";
  private static final String BOOTSTRAP_SERVERS = "10.97.136.161:9092";
  private static final String streamId = "siddhiStream1";

  public static void main(String[] args) {
    SiddhiStreamsProducer streamsProducer = new SiddhiStreamsProducer(topic, BOOTSTRAP_SERVERS, streamId);

    List<Object> firstData = Arrays.asList("Rectangle", 19.0, 19);

    List<Object> secondData = Arrays.asList("Square", 21.0, 21);

    streamsProducer.produce(firstData.toArray());
    streamsProducer.produce(secondData.toArray());

    streamsProducer.shutdown();
  }
}