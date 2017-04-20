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

import java.util.Properties;

public class SiddhiStreamsProcessorDriver {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "siddhi-streams-processor-driver");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.97.136.161:9092");
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

    StreamsConfig streamsConfig = new StreamsConfig(properties);

    JsonDeserializer<SiddhiRuleContract> siddhiRuleContractJsonDeserializer = new JsonDeserializer<>(SiddhiRuleContract.class);
    JsonSerializer<SiddhiRuleContract> siddhiRuleContractJsonSerializer = new JsonSerializer<>();

    JsonDeserializer<SiddhiStreamsContract> siddhiStreamsContractJsonDeserializer = new JsonDeserializer<>(SiddhiStreamsContract.class);
    JsonSerializer<SiddhiStreamsContract> siddhiStreamsContractJsonSerializer = new JsonSerializer<>();

    StringDeserializer stringDeserializer = new StringDeserializer();
    StringSerializer stringSerializer = new StringSerializer();

    Serde<String> siddhiRuleContractSerde = Serdes.serdeFrom(stringSerializer, stringDeserializer);

    SiddhiRuleProcessor siddhiRuleProcessor = new SiddhiRuleProcessor("siddhiStream1");

    SiddhiStreamsProcessor siddhiStreamsProcessor = new SiddhiStreamsProcessor();

    TopologyBuilder builder = new TopologyBuilder();
    builder.addSource("SOURCE", stringDeserializer, siddhiRuleContractJsonDeserializer, "siddhi-rule-topic9")
           .addSource("STREAMSOURCE", stringDeserializer, siddhiStreamsContractJsonDeserializer, "siddhi-stream-topic9")
           .addProcessor("PROCESS", new ProcessorSupplier() {
             @Override
             public Processor get() {
               return siddhiRuleProcessor;
             }
           }, "SOURCE")
           .addProcessor("STREAMPROCESS", new ProcessorSupplier() {
             @Override
             public Processor get() {
               return siddhiStreamsProcessor;
             }
           }, "STREAMSOURCE")
           .addStateStore(Stores.create("siddhi-rule-store").withStringKeys()
            .withValues(siddhiRuleContractSerde).inMemory().maxEntries(100).build(), "PROCESS", "STREAMPROCESS")
           .addSink("SINK", "siddhi-sink-topic9", stringSerializer, siddhiRuleContractJsonSerializer, "PROCESS")
           .addSink("STREAMSINK", "siddhi-stream-sink-topic9", stringSerializer, siddhiStreamsContractJsonSerializer, "STREAMPROCESS");

    KafkaStreams streaming = new KafkaStreams(builder, streamsConfig);
    streaming.start();
  }
}