/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class OpenSourceConsumer implements IConsumer {
  private final String _host;
  private final long _port;
  private final int _soTimeout;
  private final int _bufferSize;
  private final String _clientId;
  private final KafkaAvroMessageDecoder deserializer = new KafkaAvroMessageDecoder();
  private KafkaConsumer<String, String> _consumer;

  public OpenSourceConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
    _host = host;
    _port = port;
    _soTimeout = soTimeout;
    _bufferSize = bufferSize;
    _clientId = clientId;
  }
  public void start(IFactory simpleConsumerFactory,
      String bootstrapNodes, String clientId, String topic, int partition, long connectTimeoutMillis) {
    // Subscribe
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapNodes);
    // Required to be set for a new consumer. Not used since we don't commit offsets to Kafka and handle partition assignments ourselves
    props.put("group.id", "pinot-kafka");
    // We don't want to commit offets to Kafka. We handle that on our end.
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    // Becuase Pinot uses GenericRecords and LiKafka returns an IndexedRecord, we convert the row upon return from poll
    props.put("value.deserializer", "com.linkedin.pinot.core.realtime.impl.kafka.KafkaIndexedRecordToGenericRecordDecoder");
    _consumer = new KafkaConsumer<>(props);
    _consumer.subscribe(topic);
  }

  public void close() {
    _consumer.close();
  }

  public List<Pair<GenericRow, Long>> poll(long startOffset, long timeoutMillis, long maxNumberMessages, KafkaIndexedRecordToGenericRecordDecoder deserializer) throws InterruptedException {
    _consumer.poll(timeoutMillis);
    // return deserializer.decode();
    return new ArrayList<>();
  }
}
