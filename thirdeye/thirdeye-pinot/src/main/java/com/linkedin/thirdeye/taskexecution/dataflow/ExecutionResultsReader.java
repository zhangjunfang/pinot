package com.linkedin.thirdeye.taskexecution.dataflow;

public interface ExecutionResultsReader<K, V> {

  boolean hasNext();

  ExecutionResult<K, V> next();

  ExecutionResult<K, V> get(K key);

}
