package org.apache.kafka.utils;

import java.io.Serializable;
import java.util.List;

public class SiddhiStreamData implements Serializable {
  private String topic;
  private String bootstrapServers;

  private List<List<Object>> data;

  public SiddhiStreamData() {}

  public SiddhiStreamData(String topic, String bootstrapServers,
                          List<List<Object>> data) {
    this.topic = topic;
    this.bootstrapServers = bootstrapServers;
    this.data = data;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setData(List<List<Object>> data) {
    this.data = data;
  }

  public List<List<Object>> getData() {
    return data;
  }
}