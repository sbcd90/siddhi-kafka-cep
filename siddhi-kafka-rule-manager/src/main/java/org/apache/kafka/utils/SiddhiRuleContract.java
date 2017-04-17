package org.apache.kafka.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class SiddhiRuleContract implements Serializable {
  private static final Long serialVersionUID = 42L;

  private String streamId;

  private List<String> definitions;

  private String siddhiQuery;

  public SiddhiRuleContract(String streamId, List<String> definitions, String siddhiQuery) {
    this.streamId = streamId;
    this.definitions = definitions;
    this.siddhiQuery = siddhiQuery;
  }

  public void setStreamId(String streamId) {
    this.streamId = streamId;
  }

  public String getStreamId() {
    return streamId;
  }

  public void setDefinitions(List<String> definitions) {
    this.definitions = definitions;
  }

  public List<String> getDefinitions() {
    return definitions;
  }

  public void setSiddhiQuery(String siddhiQuery) {
    this.siddhiQuery = siddhiQuery;
  }

  public String getSiddhiQuery() {
    return siddhiQuery;
  }

  public String getRule() {
    if (Objects.nonNull(definitions)) {
      StringBuilder definitionsStr = new StringBuilder(String.join("\n", definitions));

      if (Objects.nonNull(siddhiQuery)) {
        StringBuilder queryStr = new StringBuilder(siddhiQuery);
        definitionsStr  = definitionsStr.append("\n").append(queryStr);
      }

      return definitionsStr.toString();
    }
    throw new RuntimeException("The Siddhi Cep Rule cannot be created");
  }
}