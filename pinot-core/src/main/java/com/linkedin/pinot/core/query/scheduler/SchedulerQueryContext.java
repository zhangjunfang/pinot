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
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.scheduler.tokenbucket.BoundedAccountingExecutor;
import com.linkedin.pinot.core.query.scheduler.tokenbucket.TableTokenAccount;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SchedulerQueryContext {
  private static Logger LOGGER = LoggerFactory.getLogger(SchedulerQueryContext.class);

  private final QueryRequest queryRequest;

  private final ListenableFutureTask<DataTable> queryFutureTask;
  private final ListenableFuture<byte[]> queryResultFuture;
  private TableTokenAccount tableAccountant;
  private BoundedAccountingExecutor executor;

  public SchedulerQueryContext(@Nonnull QueryRequest queryRequest, @Nonnull ListenableFutureTask<DataTable> queryFutureTask,
      @Nonnull ListenableFuture<byte[]> queryResultFuture) {
    Preconditions.checkNotNull(queryRequest);
    Preconditions.checkNotNull(queryFutureTask);
    Preconditions.checkNotNull(queryResultFuture);

    this.queryRequest = queryRequest;
    this.queryFutureTask = queryFutureTask;
    this.queryResultFuture = queryResultFuture;
  }

  public @Nonnull QueryRequest getQueryRequest() {
    return queryRequest;
  }

  public @Nonnull ListenableFutureTask<DataTable> getQueryFutureTask() {
    return queryFutureTask;
  }

  public @Nonnull ListenableFuture<byte[]> getQueryResultFuture() {
    return queryResultFuture;
  }

  public int getMaxWorkerThreads() {
    return 8;
  }

  public void setTableAccountant(TableTokenAccount tableAccountant) {
    this.tableAccountant = tableAccountant;
  }

  public @Nullable TableTokenAccount getTableAccountant() {
    return tableAccountant;
  }

  public void setExecutor(BoundedAccountingExecutor executor) {
    this.executor = executor;
  }

  public BoundedAccountingExecutor getExecutor() {
    return executor;
  }
}
