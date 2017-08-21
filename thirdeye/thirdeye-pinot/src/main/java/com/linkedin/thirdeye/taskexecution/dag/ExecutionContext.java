package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Map;

public interface ExecutionContext<T extends ExecutionResult> {

  NodeIdentifier getNodeIdentifier();

  void setNodeIdentifier(NodeIdentifier nodeIdentifier);

  Map<NodeIdentifier, ? extends ExecutionResult> getInputs();

  void addResult(NodeIdentifier identifier, T result);

}
