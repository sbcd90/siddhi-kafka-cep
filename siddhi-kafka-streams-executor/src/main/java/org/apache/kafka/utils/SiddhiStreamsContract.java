package org.apache.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.List;

public class SiddhiStreamsContract implements Serializable {
  private static final Long serialVersionUID = 42L;

  private String streamId;

  private List<Object> data;

  public SiddhiStreamsContract() {

  }

  public SiddhiStreamsContract(String streamId, List<Object> data) {
    this.streamId = streamId;
    this.data = data;
  }

  public void setStreamId(String streamId) {
    this.streamId = streamId;
  }

  public String getStreamId() {
    return streamId;
  }

  public void setData(List<Object> data) {
    this.data = data;
  }

  public List<Object> getData() {
    return data;
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