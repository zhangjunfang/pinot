package com.linkedin.thirdeye.taskexecution.dataflow;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ExecutionResults<K, V> {
  private NodeIdentifier nodeIdentifier;
  private Map<K, ExecutionResult<K, V>> results = new HashMap<>();

  public ExecutionResults(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  /**
   * Adds {@link ExecutionResult} to the result map. The key and value are retrieved from the result directly.
   *
   * @param executionResult the {@link ExecutionResult} to be added.
   *
   * @return the previous {@link ExecutionResult} that is associated with the key.
   */
  public ExecutionResult<K, V> addResult(ExecutionResult<K, V> executionResult) {
    K key = executionResult.key();
    ExecutionResult<K, V> previousResult = results.get(key);
    results.put(key, executionResult);
    return previousResult;
  }

  /**
   * Returns the {@link ExecutionResult} that is associated with the given key.
   *
   * @param key the key to look up the execution results.
   *
   * @return the {@link ExecutionResult} that is associated with the the given key.
   */
  public ExecutionResult<K, V> getResult(K key) {
    return results.get(key);
  }

  /**
   * Returns the key set of the result map.
   *
   * @return the key set of the result map.
   */
  public Set<K> keySet() {
    return results.keySet();
  }

  /**
   * Returns the number of results that is stored in this collection of results.
   *
   * @return the number of results that is stored in this collection of results.
   */
  public int size() {
    return results.size();
  }
}
