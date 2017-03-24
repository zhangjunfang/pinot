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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class TableTokenAccount {
  private static Logger LOGGER = LoggerFactory.getLogger(TableTokenAccount.class);

  private final String tableName;
  private final int tokenLifetimeMs;
  private final int numTokensPerMs;

  private int availableTokens;
  private long lastUpdateTimeMs;
  private int threadsInUse;
  private final List<SchedulerQueryContext> pendingQueries = new LinkedList<>();

  private final Lock tokenLock = new ReentrantLock();

  TableTokenAccount(String tableName, int numTokensPerMs, int tokenLifetimeMs) {
    this.tableName = tableName;
    this.numTokensPerMs = numTokensPerMs;
    this.tokenLifetimeMs = tokenLifetimeMs;
    lastUpdateTimeMs = System.currentTimeMillis();
    availableTokens = numTokensPerMs * tokenLifetimeMs;
  }

  int getAvailableTokens() {
    tokenLock.lock();
    try {
      consumeTokens();
      return availableTokens;
    } finally {
      tokenLock.unlock();
    }
  }

  public void incrementThreads() {
    tokenLock.lock();
    try {
      consumeTokens();
      ++threadsInUse;
    } finally {
      tokenLock.unlock();
    }
  }

  public void decrementThreads() {
    tokenLock.lock();
    try {
      consumeTokens();
      --threadsInUse;
    } finally {
      tokenLock.unlock();
    }
  }

  public List<SchedulerQueryContext> getPendingQueries() {
    return pendingQueries;
  }

  // callers must synchronize access to this method
  private void consumeTokens() {
    long currentTimeMs = System.currentTimeMillis();
    // multiple time qantas may have elapsed..hence, the modulo operation
    long diffMs = (currentTimeMs - lastUpdateTimeMs);
    if (diffMs >= tokenLifetimeMs) {
      availableTokens = tokenLifetimeMs * (numTokensPerMs - threadsInUse);
    } else {
      availableTokens -= diffMs * threadsInUse;
    }
  }
}
