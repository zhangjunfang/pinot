package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryExecutionResultsReader<K, V> implements ExecutionResultsReader<K, V> {
  private ExecutionResults<K, V> executionResults;
  private List<K> keyList = Collections.emptyList();
  private int idx = 0;

  public InMemoryExecutionResultsReader() {
  }

  public InMemoryExecutionResultsReader(ExecutionResults<K, V> executionResults) {
    this.executionResults = executionResults;
    keyList = new ArrayList<>(executionResults.keySet());
  }

  @Override
  public boolean hasNext() {
    return idx < keyList.size();
  }

  @Override
  public ExecutionResult<K, V> next() {
    return executionResults.getResult(keyList.get(idx++));
  }

  @Override
  public ExecutionResult<K, V> get(K key) {
    return executionResults.getResult(key);
  }
}
