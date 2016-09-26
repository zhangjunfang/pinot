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

package com.linkedin.pinot.core.query.scheduler.tokenbucket;

import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PriorityQueryQueue implements SchedulerPriorityQueue {

  private static Logger LOGGER = LoggerFactory.getLogger(PriorityQueryQueue.class);
  private static final int DEFAULT_TOKEN_REPLENISH_INTERVAL_MS = 100;
  private static final int DEFAULT_TOKENS_PER_INTERVAL;

  private final Map<String, TableTokenAccount> tableSchedulerInfo = new HashMap<>();

  private final Lock queueLock = new ReentrantLock();
  private final Condition queryReaderConditon = queueLock.newCondition();

  static {
    DEFAULT_TOKENS_PER_INTERVAL = Runtime.getRuntime().availableProcessors() * DEFAULT_TOKEN_REPLENISH_INTERVAL_MS;
  }

  @Override
  public void put(SchedulerQueryContext query) {
    String tableName = query.getQueryRequest().getInstanceRequest().getQuery().getQuerySource().getTableName();
    try {
      queueLock.lock();
      TableTokenAccount tableInfo = tableSchedulerInfo.get(tableName);
      if (tableInfo == null) {
        tableInfo = tableSchedulerInfo.put(tableName, new TableTokenAccount(tableName, DEFAULT_TOKENS_PER_INTERVAL));
      }
      tableInfo.getPendingQueries().add(query);
      queryReaderConditon.notifyAll();
    } finally {
      queueLock.unlock();
    }
  }

  /**
   * Blocking call to read the next query in order of priority
   * @return
   */

  @Override
  public SchedulerQueryContext take() {
    queueLock.lock();

    while (true) {
      SchedulerQueryContext schedulerQueryContext = takeNextQuery();
      return schedulerQueryContext;
    }
  }

  private SchedulerQueryContext takeNextQuery() {
    return null;
  }

  private SchedulerQueryContext takeNextInternal() {
    String selectedTableName;
    int selectedTokens = -1;
    long selectedQueryArrivalTime = Long.MAX_VALUE;
    SchedulerQueryContext selectedQuery = null;

    for (Map.Entry<String, TableTokenAccount> tableInfoEntry : tableSchedulerInfo.entrySet()) {
      String tableName = tableInfoEntry.getKey();
      TableTokenAccount tableInfo = tableInfoEntry.getValue();
      if (tableInfo.getAvailableTokens() < selectedTokens) {
        continue;
      }
      SchedulerQueryContext candidateQuery = tableInfo.getPendingQueries().get(0);
      if (tableInfo.getAvailableTokens() > selectedTokens) {
        selectedTableName = tableName;
        selectedTokens = tableInfo.getAvailableTokens();
        // TODO:
        //selectedQueryArrivalTime = tableInfo.getPendingQueries().get(0).
        selectedQuery = candidateQuery;
      }
      // else current table tokens are equal to selected.
      // select the one with earlier arrival time

    }
  }
}

