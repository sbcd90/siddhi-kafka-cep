package org.apache.kafka.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class SiddhiRule implements Serializable {
  private String topic;
  private String bootstrapServers;
  private ArrayList<String> definitions;

  private String rule;

  public SiddhiRule() {}

  public SiddhiRule(String topic, String bootstrapServers,
                    ArrayList<String> definitions, String rule) {
    this.topic = topic;
    this.bootstrapServers = bootstrapServers;
    this.definitions = definitions;
    this.rule = rule;
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

  public void setDefinitions(ArrayList<String> definitions) {
    this.definitions = definitions;
  }

  public ArrayList<String> getDefinitions() {
    return definitions;
  }

  public void setRule(String rule) {
    this.rule = rule;
  }

  public String getRule() {
    return rule;
  }
}