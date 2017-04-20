package org.apache.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

public class JsonDeserializer<T> implements Deserializer<T> {

  private ObjectMapper mapper;
  private Class<T> deserializedClass;

  public JsonDeserializer() {
    this.mapper = new ObjectMapper();
  }

  public JsonDeserializer(Class<T> deserializedClass) {
    this.mapper = new ObjectMapper();
    this.deserializedClass = deserializedClass;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> map, boolean b) {
    if (Objects.isNull(deserializedClass)) {
      deserializedClass = (Class<T>) map.get("serializedClass");
    }
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    try {
      if (Objects.isNull(bytes)) {
        return null;
      }

      return mapper.readValue(bytes, deserializedClass);
    } catch (Exception e) {
      RuntimeException re = new RuntimeException("Json deserializing failed");
      re.initCause(e);
      throw re;
    }
  }

  @Override
  public void close() {

  }
}