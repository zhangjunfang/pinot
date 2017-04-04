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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PriorityQueryQueue implements SchedulerPriorityQueue {

  private static Logger LOGGER = LoggerFactory.getLogger(PriorityQueryQueue.class);

  private final Map<String, TableTokenAccount> tableSchedulerInfo = new HashMap<>();

  private final Lock queueLock = new ReentrantLock();
  private final Condition queryReaderCondition = queueLock.newCondition();
  private final int tokenLifetimeMs;
  private final int tokensPerMs;

  public PriorityQueryQueue(int tokensPerMs, int tokenLifetimeMs) {
    this.tokensPerMs = tokensPerMs;
    this.tokenLifetimeMs = tokenLifetimeMs;
  }

  @Override
  public void put(@Nonnull SchedulerQueryContext query) {
    Preconditions.checkNotNull(query);
    String tableName = query.getQueryRequest().getTableName();
    queueLock.lock();
    try {
      TableTokenAccount tableInfo = tableSchedulerInfo.get(tableName);
      if (tableInfo == null) {
        tableInfo = new TableTokenAccount(tableName, tokensPerMs, tokenLifetimeMs);
        tableSchedulerInfo.put(tableName, tableInfo);
      }
      query.setTableAccountant(tableInfo);
      tableInfo.getPendingQueries().add(query);
      queryReaderCondition.signal();
    } finally {
      queueLock.unlock();
    }
  }

  /**
   * Blocking call to read the next query in order of priority
   * @return
   */
  @Override
  public @Nonnull SchedulerQueryContext take() {
    queueLock.lock();
    try {
      while (true) {
        SchedulerQueryContext schedulerQueryContext = null;
        while ( (schedulerQueryContext = takeNextInternal()) == null) {
          queryReaderCondition.awaitUninterruptibly();
        }
        return schedulerQueryContext;
      }
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void markTaskDone(@Nonnull SchedulerQueryContext queryContext) {
    queueLock.lock();
    try {
      TableTokenAccount tableTokenAccount = tableSchedulerInfo.get(queryContext.getQueryRequest().getTableName());
      if (tableTokenAccount != null) {
        tableTokenAccount.decrementThreads();
      }
      // ignore if null
    } finally {
      queueLock.unlock();
    }
  }

  private SchedulerQueryContext takeNextInternal() {
    int selectedTokens = Integer.MIN_VALUE;
    SchedulerQueryContext selectedQuery = null;

    for (Map.Entry<String, TableTokenAccount> tableInfoEntry : tableSchedulerInfo.entrySet()) {
      TableTokenAccount tableInfo = tableInfoEntry.getValue();
      if (tableInfo.getPendingQueries().isEmpty()) {
        continue;
      }
      int tableTokens = tableInfo.getAvailableTokens();
      if (tableTokens < selectedTokens) {
        continue;
      }
      if (tableTokens > selectedTokens || selectedQuery == null) {
        selectedTokens = tableTokens;
        selectedQuery = tableInfo.getPendingQueries().get(0);
      } else {
        // tokens being equal, use FCFS
        SchedulerQueryContext candidateQuery = tableInfo.getPendingQueries().get(0);
        long candidateArrivalTimeMs = candidateQuery.getQueryRequest().getTimerContext().getQueryArrivalTimeMs();
        long selectedArrivalTimeMs = selectedQuery.getQueryRequest().getTimerContext().getQueryArrivalTimeMs();
        if (candidateArrivalTimeMs <= selectedArrivalTimeMs) {
          selectedTokens = tableTokens;
          selectedQuery = tableInfo.getPendingQueries().get(0);
        }
      }
    }
    if (selectedQuery != null) {
      String selectedTable = selectedQuery.getQueryRequest().getTableName();
      TableTokenAccount tableTokenAccount = tableSchedulerInfo.get(selectedTable);
      tableTokenAccount.getPendingQueries().remove(0);
    }
    return selectedQuery;
  }
}

