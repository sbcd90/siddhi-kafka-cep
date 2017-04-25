package org.apache.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.interfaces.SiddhiStreamsConsumer;

import java.util.Arrays;
import java.util.function.Consumer;

public class SiddhiStreamsDataReceiver {
  private static final String topic = "siddhi-stream-sink-topic18";
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  public static void main(String[] args) throws Exception {

    SiddhiStreamsConsumer streamsConsumer = new SiddhiStreamsConsumer(topic, BOOTSTRAP_SERVERS);

    streamsConsumer.consume(100, new Consumer<Object[]>() {
      @Override
      public void accept(Object[] objects) {

        String[] outputData = new String[objects.length];

        int count = 0;
        for (Object data: objects) {
          outputData[count] = data.toString();
          count++;
        }
        System.out.println(StringUtils.join(Arrays.asList(outputData), " - "));
      }
    });
  }
}