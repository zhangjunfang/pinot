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


public class OpenSourceConsumer implements IConsumer {
  private final String _foo;
  private final long _bar;
  public OpenSourceConsumer(String foo, long bar) {
    _foo = foo;
  _bar = bar;
  }
  public void start() {
    // Subscribe
  }

  public void stop() {
    // Unsubscribe
  }

  public List<Pair<GenericRow, Long>> poll(long startOffset, long timeoutMillis, long maxNumberMessages, KafkaAvroMessageDecoder deserializer) throws InterruptedException {
    return new ArrayList<>();
  }
}
