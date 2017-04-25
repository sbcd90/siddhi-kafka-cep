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
    if (args.length < 6) {
      throw new RuntimeException("Please provide the right no. of arguments");
    }

    String bootstrapServers = null;
    int replicationFactor = 1;
    String ruleSourceTopic = null;
    String streamSourceTopic = null;
    String ruleSinkTopic = null;
    String streamSinkTopic = null;

    for (String arg: args) {
      if (arg.contains("--bootstrapServers=")) {
        bootstrapServers = arg.split("--bootstrapServers=")[1];
      }
      if (arg.contains("--replicationFactor=")) {
        replicationFactor = Integer.valueOf(arg.split("--replicationFactor=")[1]);
      }
      if (arg.contains("--ruleSourceTopic=")) {
        ruleSourceTopic = arg.split("--ruleSourceTopic=")[1];
      }
      if (arg.contains("--streamSourceTopic=")) {
        streamSourceTopic = arg.split("--streamSourceTopic=")[1];
      }
      if (arg.contains("--ruleSinkTopic=")) {
        ruleSinkTopic = arg.split("--ruleSinkTopic=")[1];
      }
      if (arg.contains("--streamSinkTopic=")) {
        streamSinkTopic = arg.split("--streamSinkTopic=")[1];
      }
    }

    Objects.requireNonNull(bootstrapServers, "Bootstrap servers should be provided with --bootstrapServers option");
    Objects.requireNonNull(ruleSourceTopic, "RuleSourceTopic should be provided with --ruleSourceTopic option");
    Objects.requireNonNull(streamSourceTopic, "StreamSourceTopic should be provided with --streamSourceTopic option");
    Objects.requireNonNull(ruleSinkTopic, "RuleSinkTopic should be provided with --ruleSinkTopic option");
    Objects.requireNonNull(streamSinkTopic, "StreamSinkTopic should be provided with --streamSinkTopic option");

    SiddhiStreamsProcessorDriver driver = new SiddhiStreamsProcessorDriver(bootstrapServers, replicationFactor);

    StreamsConfig streamsConfig = driver.getConfig();

    driver.fillSiddhiRuleContractSerializer();
    driver.fillSiddhiStreamsContractSerializer();
    driver.fillStringSerializer();

    TopologyBuilder builder = driver.createTopology(ruleSourceTopic, streamSourceTopic, ruleSinkTopic, streamSinkTopic);

    KafkaStreams streaming = new KafkaStreams(builder, streamsConfig);
    streaming.start();
  }
}