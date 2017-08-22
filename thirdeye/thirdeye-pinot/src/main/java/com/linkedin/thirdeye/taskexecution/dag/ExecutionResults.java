package com.linkedin.thirdeye.taskexecution.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExecutionResults<K, V> {
  private NodeIdentifier nodeIdentifier;
  private Map<K, List<ExecutionResult<K, V>>> results = new HashMap<>();

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
   * Adds execution results to the result map. The key and value are retrieved from the execution result directly.
   *
   * @param executionResult the execution result to be added.
   */
  public void addResult(ExecutionResult<K, V> executionResult) {
    K key = executionResult.getKey();
    List<ExecutionResult<K, V>> resultList = results.get(key);
    if (resultList == null) {
      resultList = new ArrayList<>();
      results.put(key, resultList);
    }
    resultList.add(executionResult);
  }

  /**
   * Returns a list of {@link ExecutionResult} that are associated with the given key.
   *
   * @param key the key to look up the execution results.
   *
   * @return an empty list if no execution results is associated with the the given key.
   */
  public List<ExecutionResult<K, V>> getResult(K key) {
    if (results.containsKey(key)) {
      return results.get(key);
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns the key set of the result map.
   *
   * @return the key set of the result map.
   */
  public Set<K> keySet() {
    return results.keySet();
  }
}
