package org.apache.kafka.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.utils.InputHandlerMap;
import org.apache.kafka.utils.SiddhiStreamsContract;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.Objects;

public class SiddhiStreamsProcessor extends AbstractProcessor<String, SiddhiStreamsContract> {

  private ProcessorContext context;
  private KeyValueStore<String, String> siddhiStreamStore;
  private InputHandlerMap inputHandlerMap;

  public SiddhiStreamsProcessor(InputHandlerMap inputHandlerMap) {
    this.inputHandlerMap = inputHandlerMap;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.context.schedule(10000);
    siddhiStreamStore = (KeyValueStore<String, String>) this.context.getStateStore("siddhi-stream-store");
    Objects.requireNonNull(siddhiStreamStore, "State store can't be null");
  }

  @Override
  public void process(String s, SiddhiStreamsContract siddhiStreamsContract) {
    try {
      siddhiStreamStore.put(s, null);
      InputHandler inputHandler = inputHandlerMap.getInputHandler(siddhiStreamsContract.getStreamId());
      Objects.requireNonNull(inputHandler);

      inputHandler.send(siddhiStreamsContract.getData().toArray());
      Thread.sleep(500);

      String recordValue = siddhiStreamStore.get(s);

      if (Objects.nonNull(recordValue)) {
        context.forward(s, recordValue);
        context.commit();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

/*  @Override
  public void punctuate(long timestamp) {
    try {
      Thread.sleep(500);
      System.out.println("Trying to write records");
      KeyValueIterator<String, String> it = siddhiStreamStore.all();
      System.out.println("Records to write - " + it.hasNext());
      long currentTime = System.currentTimeMillis();

      while (it.hasNext()) {
        KeyValue<String, String> record = it.next();
        context.forward(record.key, record.value);
      }
      it.close();
      context.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  } */

  @Override
  public void close() {
    // nothing to do
  }
}