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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BoundedAccountingExecutor implements ExecutorService {
  private static Logger LOGGER = LoggerFactory.getLogger(BoundedAccountingExecutor.class);
  private final ExecutorService delegateExecutor;
  private Semaphore semaphore;
  private final String tableName;
  private TableTokenAccount tableAccountant = null;

  public BoundedAccountingExecutor(ExecutorService s, Semaphore semaphore, String tableName) {
    this.delegateExecutor = s;
    this.semaphore = semaphore;
    this.tableName = tableName;
  }

  public BoundedAccountingExecutor(ExecutorService s, String tableName) {
    this.delegateExecutor = s;
    this.tableName = tableName;
  }

  public void setTableAccountant(TableTokenAccount accountant) {
     this.tableAccountant = accountant;
  }

  @VisibleForTesting
  public Semaphore getSemaphore() {
    return semaphore;
  }

  public void setBounds(Semaphore s) {
    this.semaphore = s;
  }

  @Override
  public void shutdown() {
    delegateExecutor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegateExecutor.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegateExecutor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegateExecutor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegateExecutor.awaitTermination(timeout,unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegateExecutor.submit(toAccountingCallable(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegateExecutor.submit(toAccoutingRunnable(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegateExecutor.submit(toAccoutingRunnable(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public void execute(Runnable command) {
    delegateExecutor.execute(toAccoutingRunnable(command));
  }

  private <T> QueryAccountingCallable<T> toAccountingCallable(Callable<T> callable) {
    acquirePermits(1);
    return new QueryAccountingCallable<>(callable, semaphore, tableAccountant);
  }

  private QueryAccountingRunnable toAccoutingRunnable(Runnable runnable) {
    acquirePermits(1);
    return new QueryAccountingRunnable(runnable, semaphore, tableAccountant);
  }

  private void acquirePermits(int permits) {
    try {
      semaphore.acquire(permits);
    } catch (InterruptedException e) {
      LOGGER.error("Thread interrupted while waiting for semaphore", e);
      throw new RuntimeException(e);
    }
  }
}

class QueryAccountingRunnable implements Runnable {
  private final Runnable runnable;
  private final Semaphore semaphore;
  private final TableTokenAccount accountant;

  QueryAccountingRunnable(Runnable r, Semaphore semaphore, TableTokenAccount accountant) {
    this.runnable = r;
    this.semaphore = semaphore;
    this.accountant = accountant;
  }

  @Override
  public void run() {
    long startTime = System.nanoTime();
    try {
      if (accountant != null) {
        accountant.incrementThreads();
      }
      runnable.run();
    } finally {
      long totalTime = System.nanoTime() - startTime;
      if (accountant != null) {
        accountant.decrementThreads();
      }
      semaphore.release();
    }
  }
}

class QueryAccountingCallable<T> implements Callable<T> {

  private final Callable<T> callable;
  private final Semaphore semaphore;
  private final TableTokenAccount accountant;

  QueryAccountingCallable(Callable<T> c, Semaphore semaphore, TableTokenAccount accountant) {
    this.callable = c;
    this.semaphore = semaphore;
    this.accountant = accountant;
  }
  @Override
  public T call()
      throws Exception {
    long startTime = System.nanoTime();
    try {
      if (accountant != null) {
        accountant.incrementThreads();
      }
      return callable.call();
    } finally {
      long totalTime = System.nanoTime();
      if (accountant != null) {
        accountant.decrementThreads();
      }
      semaphore.release();
    }
  }
}
