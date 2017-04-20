package org.apache.kafka.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.utils.SiddhiRuleActivationInfo;
import org.apache.kafka.utils.SiddhiRuleContract;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SiddhiRuleProcessor extends AbstractProcessor<String, SiddhiRuleContract> {

  private ProcessorContext context;
  private KeyValueStore<String, String> siddhiRuleStore;

  private final SiddhiManager siddhiManager;
  private final Map<String, ExecutionPlanRuntime> executionPlanRuntimes;
  private final Map<String, InputHandler> inputHandlers;

  private final String streamId;

  private QueryCallback callback;

  public SiddhiRuleProcessor(String streamId) {
    this.streamId = streamId;

    this.siddhiManager = new SiddhiManager();

    this.executionPlanRuntimes = new HashMap<>();
    this.inputHandlers = new HashMap<>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.context.schedule(10000);
    siddhiRuleStore = (KeyValueStore<String, String>) this.context.getStateStore("siddhi-rule-store");
    Objects.requireNonNull(siddhiRuleStore, "State store can't be null");
    siddhiRuleStore.flush();

/*    callback = new QueryCallback() {
      @Override
      public void receive(long l, Event[] events, Event[] events1) {
        for (Event event: events) {
          if (event.getData().length == 1) {
            String rule = event.getData()[0].toString();
            siddhiRuleStore.put(streamId, rule);
            context.forward(streamId, rule);
          }
        }
        context.commit();
      }
    }; */
  }

/*  public void addRule(SiddhiRuleContract siddhiRuleContract) {
    ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(siddhiRuleContract.getRule());

    executionPlanRuntimes.put(streamId, executionPlanRuntime);

    executionPlanRuntime.addCallback(streamId, callback);

    InputHandler inputHandler = executionPlanRuntime.getInputHandler(streamId);
    inputHandlers.put(streamId, inputHandler);
    executionPlanRuntime.start();
  } */

  @Override
  public void process(String s, SiddhiRuleContract siddhiRuleContract) {
    siddhiRuleStore.putIfAbsent(streamId, new SiddhiRuleActivationInfo(getRule(siddhiRuleContract), false).toString());
    context.forward(streamId, siddhiRuleContract);
    context.commit();
  }

  private String getRule(SiddhiRuleContract siddhiRuleContract) {
    if (Objects.nonNull(siddhiRuleContract.getDefinitions())) {
      StringBuilder definitionsStr = new StringBuilder(String.join("\n", siddhiRuleContract.getDefinitions()));

      if (Objects.nonNull(siddhiRuleContract.getSiddhiQuery())) {
        StringBuilder queryStr = new StringBuilder(siddhiRuleContract.getSiddhiQuery());
        definitionsStr  = definitionsStr.append("\n").append(queryStr);
      }

      return definitionsStr.toString();
    }
    throw new RuntimeException("The Siddhi Cep Rule cannot be created");
  }

  @Override
  public void close() {
    // nothing to do
  }
}