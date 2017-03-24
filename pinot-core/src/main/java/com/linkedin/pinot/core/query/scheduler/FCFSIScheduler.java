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

package com.linkedin.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FCFS but with table specific query runner queues. They still use the same underlying query worker pool.
 */
public class FCFSIScheduler extends QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(FCFSIScheduler.class);
  private final ConcurrentMap<String, ExecutorService> tableQueryRunnerMap = new ConcurrentHashMap<>();

  public FCFSIScheduler(@Nonnull Configuration schedulerConfig, QueryExecutor queryExecutor,
      @Nonnull ServerMetrics serverMetrics) {
    super(schedulerConfig, queryExecutor, serverMetrics);
  }

  @Nonnull
  @Override
  public ListenableFuture<byte[]> submit(@Nullable QueryRequest queryRequest) {
    Preconditions.checkNotNull(queryRequest);
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    String tableName = queryRequest.getTableName();
    ExecutorService tableExecutor = tableQueryRunnerMap.get(tableName);
    if (tableExecutor == null) {
      tableExecutor = Executors.newFixedThreadPool(numQueryRunnerThreads);
      tableQueryRunnerMap.put(tableName, tableExecutor);
    }
    ListenableFutureTask<DataTable> queryTask = getQueryFutureTask(queryRequest);
    ListenableFuture<byte[]> queryResultFuture = getQueryResultFuture(queryRequest, queryTask);
    tableExecutor.submit(queryTask);
    return queryResultFuture;
  }

  @Override
  public void start() {

  }
}
