package org.apache.kafka.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.utils.SiddhiStreamsContract;

public class SiddhiStreamsProcessor extends AbstractProcessor<String, SiddhiStreamsContract> {

  @Override
  public void init(ProcessorContext context) {
    super.init(context);
  }

  @Override
  public void process(String s, SiddhiStreamsContract siddhiStreamsContract) {

  }

  @Override
  public void close() {
    super.close();
  }
}