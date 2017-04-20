package org.apache.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

  private ObjectMapper mapper;

  public JsonSerializer() {
    this.mapper = new ObjectMapper();
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    // nothing to do
  }

  @Override
  public byte[] serialize(String s, T t) {
    try {
      return mapper.writeValueAsString(t).getBytes();
    } catch (Exception e) {
      RuntimeException re = new RuntimeException("Json parsing failed");
      re.initCause(e);
      throw re;
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}