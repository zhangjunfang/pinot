package com.linkedin.thirdeye.taskexecution.dag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExecutionResults<T> {
  private NodeIdentifier nodeIdentifier;
  private List<ExecutionResult<T>> results = new ArrayList<>();

  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  public Collection<ExecutionResult<T>> getResults() {
    return results;
  }

  public void setResults(Collection<ExecutionResult<T>> results) {
    this.results = new ArrayList<>(results);
    this.results.addAll(results);
  }

  public void addResult(ExecutionResult<T> result) {
    this.results.add(result);
  }

  public void addResults(Collection<ExecutionResult<T>> results) {
    this.results.addAll(results);
  }

}
