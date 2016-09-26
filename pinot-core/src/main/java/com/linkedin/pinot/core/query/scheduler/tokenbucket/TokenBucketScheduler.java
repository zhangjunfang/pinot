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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TokenBucketScheduler extends QueryScheduler {
  private static Logger LOGGER = LoggerFactory.getLogger(TokenBucketScheduler.class);

  private final SchedulerPriorityQueue queryQueue;

  public TokenBucketScheduler(@Nonnull Configuration schedulerConfig, QueryExecutor queryExecutor,
      ServerMetrics serverMetrics) {
    super(schedulerConfig, queryExecutor, serverMetrics);
    queryQueue = new PriorityQueryQueue();
  }

  @Override
  public ListenableFuture<byte[]> submit(@Nullable final QueryRequest queryRequest) {
    ListenableFutureTask<DataTable> queryFutureTask = getQueryFutureTask(queryRequest);
    ListenableFuture<byte[]> queryResultFuture = getQueryResultFuture(queryRequest, queryFutureTask);
    final SchedulerQueryContext schedulerQueryContext = new SchedulerQueryContext(queryRequest, queryFutureTask,
        queryResultFuture);
    queryResultFuture.addListener(new Runnable() {
      @Override
      public void run() {
        // TODO: update scheduler post task completion
      }
    }, MoreExecutors.directExecutor());

    queryQueue.put(schedulerQueryContext);
    return queryResultFuture;
  }

  @Override
  public void run() {
    Thread scheduler = new Thread(new Runnable() {
      @Override
      public void run() {
        while(true) {
          // TODO: wait for capacity and request to be available
          SchedulerQueryContext request = queryQueue.take();
          queryRunners.submit(request.getQueryFutureTask());
        }
      }
    });
    scheduler.setName("query-scheduler");
    scheduler.setPriority(Thread.MAX_PRIORITY);
    scheduler.run();
  }
}
