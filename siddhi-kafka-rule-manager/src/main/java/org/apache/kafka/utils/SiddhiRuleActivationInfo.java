package org.apache.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SiddhiRuleActivationInfo {
  private static final Long serialVersionUID = 42L;

  private String rule;

  private boolean isActivated;

  public SiddhiRuleActivationInfo() {

  }

  public SiddhiRuleActivationInfo(String rule, boolean isActivated) {
    this.rule = rule;
    this.isActivated = isActivated;
  }

  public void setRule(String rule) {
    this.rule = rule;
  }

  public String getRule() {
    return rule;
  }

  public void setActivated(boolean isActivated) {
    this.isActivated = isActivated;
  }

  public boolean getIsActivated() {
    return isActivated;
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