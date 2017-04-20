package org.apache.kafka.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SiddhiRuleContract implements Serializable {
  private static final Long serialVersionUID = 42L;

  @JsonProperty("streamId")
  private String streamId;

  @JsonProperty("definitions")
  private ArrayList<String> definitions;

  @JsonProperty("siddhiQuery")
  private String siddhiQuery;

  public SiddhiRuleContract() {

  }

  public SiddhiRuleContract(String streamId, ArrayList<String> definitions, String siddhiQuery) {
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

  public void setDefinitions(ArrayList<String> definitions) {
    this.definitions = definitions;
  }

  public ArrayList<String> getDefinitions() {
    return definitions;
  }

  public void setSiddhiQuery(String siddhiQuery) {
    this.siddhiQuery = siddhiQuery;
  }

  public String getSiddhiQuery() {
    return siddhiQuery;
  }

  @Override
  public String toString() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(this);
    } catch (Exception e) {
      RuntimeException re = new RuntimeException("Json parsing of Object failed");
      re.initCause(e);
      throw re;
    }
  }
}