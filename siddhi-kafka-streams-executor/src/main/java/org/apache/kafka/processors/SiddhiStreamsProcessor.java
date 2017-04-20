package org.apache.kafka.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.utils.SiddhiRuleActivationInfo;
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
    try {
      ObjectMapper mapper = new ObjectMapper();
      SiddhiRuleActivationInfo ruleActivationInfo =
        mapper.readValue(siddhiRuleStore.get(siddhiStreamsContract.getStreamId()), SiddhiRuleActivationInfo.class);
      System.out.println(ruleActivationInfo.getRule() + " - " + ruleActivationInfo.getIsActivated());

      ruleActivationInfo.setActivated(true);

      siddhiRuleStore.put(siddhiStreamsContract.getStreamId(), ruleActivationInfo.toString());

      SiddhiRuleActivationInfo updatedRuleActivationInfo =
        mapper.readValue(siddhiRuleStore.get(siddhiStreamsContract.getStreamId()), SiddhiRuleActivationInfo.class);
      System.out.println(updatedRuleActivationInfo.getRule() + " - " + updatedRuleActivationInfo.getIsActivated());
      context.forward(s, siddhiStreamsContract);
      context.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}