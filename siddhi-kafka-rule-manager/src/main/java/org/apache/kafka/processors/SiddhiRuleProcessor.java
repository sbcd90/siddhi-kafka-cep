package org.apache.kafka.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.utils.InputHandlerMap;
import org.apache.kafka.utils.SiddhiRuleContract;
import org.apache.kafka.utils.SiddhiStreamsContract;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;

public class SiddhiRuleProcessor extends AbstractProcessor<String, SiddhiRuleContract> {

  private ProcessorContext context;
  private KeyValueStore<String, String> siddhiStreamStore;

  private final SiddhiManager siddhiManager;
  private final Map<String, ExecutionPlanRuntime> executionPlanRuntimes;
  private final InputHandlerMap inputHandlerMap;

  private QueryCallback callback;

  public SiddhiRuleProcessor(InputHandlerMap inputHandlerMap) {

    this.siddhiManager = new SiddhiManager();

    this.executionPlanRuntimes = new HashMap<>();
    this.inputHandlerMap = inputHandlerMap;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.context.schedule(10000);
    siddhiStreamStore = (KeyValueStore<String, String>) this.context.getStateStore("siddhi-stream-store");
    Objects.requireNonNull(siddhiStreamStore, "State store can't be null");
    siddhiStreamStore.flush();
  }

  @Override
  public void process(String s, SiddhiRuleContract siddhiRuleContract) {
    ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(getRule(siddhiRuleContract));

    executionPlanRuntimes.put(siddhiRuleContract.getStreamId(), executionPlanRuntime);

    callback = new QueryCallback() {
      @Override
      public void receive(long l, Event[] events, Event[] events1) {
        for (Event event: events) {
          SiddhiStreamsContract siddhiStreamsContract =
            new SiddhiStreamsContract(siddhiRuleContract.getStreamId(), Arrays.asList(event.getData()));
          siddhiStreamStore.put(s, siddhiStreamsContract.toString());
        }
      }
    };

    executionPlanRuntime.addCallback("query1", callback);

    InputHandler inputHandler = executionPlanRuntime.getInputHandler(siddhiRuleContract.getStreamId());
    inputHandlerMap.addInputHandler(siddhiRuleContract.getStreamId(), inputHandler);

    executionPlanRuntime.start();

    context.forward(siddhiRuleContract.getStreamId(), siddhiRuleContract);
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
    try {
      for (Map.Entry<String, ExecutionPlanRuntime> executionPlanRuntime: executionPlanRuntimes.entrySet()) {
        executionPlanRuntime.getValue().shutdown();
      }
      siddhiManager.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}