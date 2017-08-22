package com.linkedin.thirdeye.taskexecution.dag;

import java.util.List;
import java.util.Map;

public interface ExecutionContext<T extends ExecutionResult> {

  NodeIdentifier getNodeIdentifier();

  void setNodeIdentifier(NodeIdentifier nodeIdentifier);

  Map<NodeIdentifier, List<T>> getInputs();

  void addResult(NodeIdentifier identifier, List<T> result);

}
