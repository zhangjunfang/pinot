package com.linkedin.thirdeye.taskexecution.dag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OperatorResults<T> {
  private NodeIdentifier nodeIdentifier;
  private List<OperatorResult<T>> results = new ArrayList<>();

  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  public Collection<OperatorResult<T>> getResults() {
    return results;
  }

  public void setResults(Collection<OperatorResult<T>> results) {
    this.results = new ArrayList<>(results);
    this.results.addAll(results);
  }

  public void addResult(OperatorResult<T> result) {
    this.results.add(result);
  }

  public void addResults(Collection<OperatorResult<T>> results) {
    this.results.addAll(results);
  }

}
