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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class QueryExecutorService extends ThreadPoolExecutor {

  public QueryExecutorService(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);

  }
}
/*

class BoundedQueryExecutorService implements ExecutorService {

  private final QueryExecutorService executor;
  private final Semaphore semaphore;

  BoundedQueryExecutorService(QueryExecutorService s, Semaphore semaphore) {
    this.executor = s;
    this.semaphore = semaphore;
  }

  @Override
  public void shutdown() {

  }

  @Override
  public List<Runnable> shutdownNow() {
    return null;
  }

  @Override
  public boolean isShutdown() {
    return false;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return false;
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    try {
      if (semaphore.tryAcquire(3, TimeUnit.SECONDS)) {
        return executor.submit(task);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("error");
    }
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return null;
  }

  @Override
  public Future<?> submit(Runnable task) {
    return null;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return null;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  @Override
  public void execute(Runnable command) {

  }
}
*/
