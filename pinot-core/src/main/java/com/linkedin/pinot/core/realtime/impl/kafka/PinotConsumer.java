package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.List;


public interface PinotConsumer {

  /**
   * Tells the Consumer to connect to the underlying system, and prepare
   * to begin serving messages when poll is invoked.
   */
  void start();

  /**
   * Tells the Consumer to close all connections, release all resource,
   * and shut down everything. The SystemConsumer will not be used again after
   * stop is called.
   */
  void stop();

  /**
   * Poll the SystemConsumer to get any available messages from the underlying
   * system.
   *
   * TODO

   * @return
   * @throws InterruptedException
   *          Thrown when a blocking poll has been interrupted by another
   *          thread.
   */
  List<Pair<GenericRow, Long>> poll(long startOffset, long timeoutMillis, int maxNumberMessages) throws InterruptedException;
}