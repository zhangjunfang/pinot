package com.linkedin.thirdeye.taskexecution.dataflow;

import java.util.Objects;

public class ExecutionResult<K, V> {
  private K key;
  private V result;

  public ExecutionResult() {
  }

  public ExecutionResult(K key, V result) {
    this.key = key;
    this.result = result;
  }

  public void setResult(K key, V result) {
    this.key = key;
    this.result = result;
  }

  public K key() {
    return key;
  }

  public V result() {
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecutionResult<?, ?> that = (ExecutionResult<?, ?>) o;
    return Objects.equals(key, that.key) && Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }
}
