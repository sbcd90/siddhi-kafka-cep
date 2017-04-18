package org.apache.kafka.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.utils.SiddhiStreamsContract;

import java.util.Objects;

public class SiddhiStreamsProcessor extends AbstractProcessor<String, SiddhiStreamsContract> {

  private ProcessorContext context;
  private KeyValueStore<String, String> siddhiRuleStore;

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.context.schedule(10000);
    siddhiRuleStore = (KeyValueStore<String, String>) this.context.getStateStore("siddhi-rule-store");
    Objects.requireNonNull(siddhiRuleStore, "State store can't be null");
  }

  @Override
  public void process(String s, SiddhiStreamsContract siddhiStreamsContract) {
    KeyValueIterator<String, String> keyValueIterator = siddhiRuleStore.all();
    while (keyValueIterator.hasNext()) {
      context.forward(s, keyValueIterator.next().value);
    }
    context.commit();
  }

  @Override
  public void close() {
    // nothing to do
  }
}