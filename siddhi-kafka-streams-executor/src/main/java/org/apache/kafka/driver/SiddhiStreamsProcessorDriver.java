package org.apache.kafka.driver;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.processors.SiddhiRuleProcessor;
import org.apache.kafka.processors.SiddhiStreamsProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.utils.*;

import java.util.Objects;
import java.util.Properties;

public class SiddhiStreamsProcessorDriver {

  private String bootstrapServers;
  private int replicationFactor;

  private JsonDeserializer<SiddhiRuleContract> siddhiRuleContractJsonDeserializer;
  private JsonSerializer<SiddhiRuleContract> siddhiRuleContractJsonSerializer;

  private JsonDeserializer<SiddhiStreamsContract> siddhiStreamsContractJsonDeserializer;
  private JsonSerializer<SiddhiStreamsContract> siddhiStreamsContractJsonSerializer;

  private StringDeserializer stringDeserializer;
  private StringSerializer stringSerializer;

  public SiddhiStreamsProcessorDriver(String bootstrapServers,
                                      int replicationFactor) {
    Objects.requireNonNull(bootstrapServers, "Bootstrap servers should point to valid kafka brokers location");

    this.bootstrapServers = bootstrapServers;

    if (replicationFactor == 0) {
      this.replicationFactor = 1;
    } else {
      this.replicationFactor = replicationFactor;
    }
  }

  private StreamsConfig getConfig() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "siddhi-streams-processor-driver");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);

    return new StreamsConfig(properties);
  }

  private void fillSiddhiRuleContractSerializer() {
    if (Objects.isNull(siddhiRuleContractJsonDeserializer) && Objects.isNull(siddhiRuleContractJsonSerializer)) {
      siddhiRuleContractJsonDeserializer = new JsonDeserializer<>(SiddhiRuleContract.class);
      siddhiRuleContractJsonSerializer = new JsonSerializer<>();
    }
  }

  private void fillSiddhiStreamsContractSerializer() {
    if (Objects.isNull(siddhiStreamsContractJsonDeserializer) && Objects.isNull(siddhiStreamsContractJsonSerializer)) {
      siddhiStreamsContractJsonDeserializer = new JsonDeserializer<>(SiddhiStreamsContract.class);
      siddhiStreamsContractJsonSerializer = new JsonSerializer<>();
    }
  }

  private void fillStringSerializer() {
    if (Objects.isNull(stringDeserializer) && Objects.isNull(stringSerializer)) {
      stringDeserializer = new StringDeserializer();
      stringSerializer = new StringSerializer();
    }
  }

  private Serde<String> getRuleContractSerde() {
    return Serdes.serdeFrom(stringSerializer, stringDeserializer);
  }

  private SiddhiRuleProcessor getSiddhiRuleProcessor() {
    InputHandlerMap inputHandlerMap = InputHandlerMap.getInstance();
    return new SiddhiRuleProcessor(inputHandlerMap);
  }

  private SiddhiStreamsProcessor getSiddhiStreamsProcessor() {
    InputHandlerMap inputHandlerMap = InputHandlerMap.getInstance();
    return new SiddhiStreamsProcessor(inputHandlerMap);
  }

  private TopologyBuilder createTopology(String ruleSourceTopic, String streamSourceTopic,
                              String ruleSinkTopic, String streamSinkTopic) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.addSource("RULESOURCE", stringDeserializer, siddhiRuleContractJsonDeserializer, ruleSourceTopic)
           .addSource("STREAMSOURCE", stringDeserializer, siddhiStreamsContractJsonDeserializer, streamSourceTopic)
           .addProcessor("RULEPROCESSOR", new ProcessorSupplier() {
             @Override
             public Processor get() {
               return getSiddhiRuleProcessor();
             }
           }, "RULESOURCE")
           .addProcessor("STREAMPROCESSOR", new ProcessorSupplier() {
             @Override
             public Processor get() {
               return getSiddhiStreamsProcessor();
             }
           }, "STREAMSOURCE")
           .addStateStore(Stores.create("siddhi-stream-store").withStringKeys()
            .withValues(getRuleContractSerde()).inMemory().maxEntries(100).build(), "RULEPROCESSOR", "STREAMPROCESSOR")
           .addSink("RULESINK", ruleSinkTopic, stringSerializer, siddhiRuleContractJsonSerializer, "RULEPROCESSOR")
           .addSink("STREAMSINK", streamSinkTopic, stringSerializer, siddhiStreamsContractJsonSerializer, "STREAMPROCESSOR");

    return builder;
  }


  public static void main(String[] args) {
    SiddhiStreamsProcessorDriver driver = new SiddhiStreamsProcessorDriver("10.97.136.161:9092", 0);

    StreamsConfig streamsConfig = driver.getConfig();

    driver.fillSiddhiRuleContractSerializer();
    driver.fillSiddhiStreamsContractSerializer();
    driver.fillStringSerializer();

    TopologyBuilder builder = driver.createTopology("siddhi-rule-topic16",
      "siddhi-stream-topic16", "siddhi-sink-topic16", "siddhi-stream-sink-topic16");

    KafkaStreams streaming = new KafkaStreams(builder, streamsConfig);
    streaming.start();
  }
}