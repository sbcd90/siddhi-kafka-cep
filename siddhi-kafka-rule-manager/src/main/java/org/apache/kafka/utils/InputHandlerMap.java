package org.apache.kafka.utils;

import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class InputHandlerMap {
  private static final Long serialVersionUID = 42L;

  private Map<String, InputHandler> inputHandlers;

  private static InputHandlerMap inputHandlerMap;

  public static InputHandlerMap getInstance() {
    if (Objects.isNull(inputHandlerMap)) {
      inputHandlerMap = new InputHandlerMap();
    }
    return inputHandlerMap;
  }

  private InputHandlerMap() {
    this.inputHandlers = new HashMap<>();
  }

  public void addInputHandler(String streamId, InputHandler inputHandler) {
    inputHandlers.put(streamId, inputHandler);
  }

  public InputHandler getInputHandler(String streamId) {
    return inputHandlers.get(streamId);
  }
}