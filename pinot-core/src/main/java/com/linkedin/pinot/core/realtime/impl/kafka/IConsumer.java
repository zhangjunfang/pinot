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
import java.util.List;


public interface IConsumer {

  /**
   * Tells the Consumer to connect to the underlying system, and prepare
   * to begin serving messages when poll is invoked.
   */
  void start(IFactory simpleConsumerFactory,
      String bootstrapNodes, String clientId, String topic, int partition, long connectTimeoutMillis);

  /**
   * Tells the Consumer to close all connections, release all resource,
   * and shut down everything. The SystemConsumer will not be used again after
   * stop is called.
   */
  void close();

  /**
   * Poll the SystemConsumer to get any available messages from the underlying
   * system.
   *
   * TODO

   * @return
   * @throws InterruptedException
   *          Thrown when a blocking poll has been interrupted by another
   *          thread.
   */
  List<Pair<GenericRow, Long>> poll(long startOffset, long endOffset, long timeoutMillis, KafkaIndexedRecordToGenericRecordDecoder deserializer) throws InterruptedException;
}
