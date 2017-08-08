package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.Map;


public class KafkaIndexedRecordToGenericRecordDecoder implements KafkaMessageDecoder {

  public void init(Map<String, String> props, Schema indexingSchema, String kafkaTopicName) throws Exception {
  }

  public GenericRow decode(byte[] payload, GenericRow destination) {
    return new GenericRow();
  }

  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return new GenericRow();
  }
}
