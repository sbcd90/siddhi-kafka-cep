package org.apache.kafka;

import org.apache.kafka.interfaces.SiddhiStreamsConsumer;

import java.util.function.Consumer;

public class SiddhiStreamsDataReceiver {
  private static final String topic = "siddhi-stream-sink-topic16";
  private static final String BOOTSTRAP_SERVERS = "10.97.136.161:9092";

  public static void main(String[] args) throws Exception {

    SiddhiStreamsConsumer streamsConsumer = new SiddhiStreamsConsumer(topic, BOOTSTRAP_SERVERS);

    streamsConsumer.consume(100, new Consumer<Object[]>() {
      @Override
      public void accept(Object[] objects) {
        for (Object data: objects) {
          System.out.println(data);
        }
      }
    });
  }
}