package org.apache.kafka;

import org.apache.kafka.interfaces.SiddhiRuleProducer;

import java.util.ArrayList;

public class SiddhiRuleGenerator {
  private static final String topic = "siddhi-rule-topic18";
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  public static void main(String[] args) {
    SiddhiRuleProducer ruleProducer = new SiddhiRuleProducer(topic, BOOTSTRAP_SERVERS);

    String streamId = "siddhiStream1";

    ArrayList<String> definitions = new ArrayList<>();
    String definition = "define stream siddhiStream1 (symbol string, price double, volume long);";
    definitions.add(definition);

    String siddhiQuery = "@info(name = 'query1') from siddhiStream1[price < 20.0] " +
      "select symbol, price, volume insert into outputStream";

    ruleProducer.createRule(streamId, definitions, siddhiQuery);

    ruleProducer.shutdown();
  }
}