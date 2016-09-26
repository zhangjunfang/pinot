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

/*

public class PriorityQueryQueue2 implements SchedulerPriorityQueue {

  private static Logger LOGGER = LoggerFactory.getLogger(PriorityQueryQueue2.class);
  private static final int DEFAULT_STRIPED_LOCKS = 20;
  private static final int DEFAULT_TOKEN_REPLENISH_INTERVAL_MS = 100;
  private static final int DEFAULT_TOKENS_PER_INTERVAL;

  private final ConcurrentMap<String, TableTokenAccount> tableSchedulerInfo = new ConcurrentHashMap<>();
  private final Striped<Lock> tableLocks = Striped.lock(DEFAULT_STRIPED_LOCKS);

  private final Lock queryReaderLock = new ReentrantLock();
  private final Condition queryReaderCondition = queryReaderLock.newCondition();

  static  {
    DEFAULT_TOKENS_PER_INTERVAL = Runtime.getRuntime().availableProcessors() * DEFAULT_TOKEN_REPLENISH_INTERVAL_MS;
  }

  PriorityQueryQueue2() {

  }

  @Override
  public void put(SchedulerQueryContext query) {

    String tableName = query.getQueryRequest().getInstanceRequest().getQuery()
        .getQuerySource().getTableName();
    TableTokenAccount tableInfo = tableSchedulerInfo.get(tableName);
    if (tableInfo == null) {
      tableInfo = tableSchedulerInfo.putIfAbsent(tableName, new TableTokenAccount(tableName, DEFAULT_TOKENS_PER_INTERVAL));
    }
    Lock tlock = tableLocks.get(tableName);
    tlock.lock();
    try {
      tableInfo.getPendingQueries().add(query);
      queryReaderLock.lock();
      queryReaderCondition.signal();
    } finally {
      tlock.unlock();
      queryReaderLock.unlock();
    }
  }

  */
/**
   * Blocking call to read the next query in order of priority
   * @return
   *//*

  @Override
  public SchedulerQueryContext take() {
    while (true) {
      try {
        SchedulerQueryContext schedulerQueryContext = takeNextQuery();
        return schedulerQueryContext;
      } catch (InterruptedException e) {
        LOGGER.warn("Interruped while waiting for queries", e);
      }
    }
  }

  private SchedulerQueryContext takeNextQuery()
      throws InterruptedException {
    SchedulerQueryContext schedulerQueryContext = takeNextInternal();

    if (schedulerQueryContext != null) {
      return schedulerQueryContext;
    }
    try {
      queryReaderLock.lock();
      schedulerQueryContext = takeNextInternal();
      if (schedulerQueryContext != null) {
        return schedulerQueryContext;
      }

      queryReaderCondition.await();
      schedulerQueryContext = takeNextInternal();
      return schedulerQueryContext;
    } finally {
      queryReaderLock.unlock();
    }
  }

  private SchedulerQueryContext takeNextInternal() {
    return null;
  }
}
*/
