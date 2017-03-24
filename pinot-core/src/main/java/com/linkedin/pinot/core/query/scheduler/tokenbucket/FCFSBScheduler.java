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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FCFSBScheduler extends QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(FCFSBScheduler.class);
  private final ConcurrentMap<String, ExecutorService> tableQueryRunnerMap = new ConcurrentHashMap<>();

  public static final int MAX_THREAD_LIMIT = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
  public static final String THREADS_PER_QUERY_PCT = "threads_per_query_pct";
  private final int maxThreadsPerQuery;

  public FCFSBScheduler(@Nonnull Configuration schedulerConfig, QueryExecutor queryExecutor,
      @Nonnull ServerMetrics serverMetrics) {
    super(schedulerConfig, queryExecutor, serverMetrics);
    int tpqPct = schedulerConfig.getInt(THREADS_PER_QUERY_PCT, 30);
    if (tpqPct > 1 &&  tpqPct <= 100) {
      maxThreadsPerQuery = Math.max(1, numQueryWorkerThreads * tpqPct / 100);
      LOGGER.info("numQuerWorkers: {}, pct: {}, max: {}", numQueryWorkerThreads, tpqPct, maxThreadsPerQuery);
    } else {
      LOGGER.error("Invalid value for {}, using default: {}", THREADS_PER_QUERY_PCT, MAX_THREAD_LIMIT);
      maxThreadsPerQuery = MAX_THREAD_LIMIT;
    }
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
    final BoundedAccountingExecutor boundedWorkerPool = new BoundedAccountingExecutor(getWorkerExecutorService(),
        new Semaphore(maxThreadsPerQuery),
        queryRequest.getTableName());
    ListenableFutureTask<DataTable> queryTask = getQueryFutureTask(queryRequest, boundedWorkerPool);
    ListenableFuture<byte[]> queryResultFuture = getQueryResultFuture(queryRequest, queryTask);
    tableExecutor.submit(queryTask);
    return queryResultFuture;
  }


  @Override
  public void start() {

  }
}
