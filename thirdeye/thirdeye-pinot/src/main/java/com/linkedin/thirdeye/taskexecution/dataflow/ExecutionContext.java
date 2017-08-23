package com.linkedin.thirdeye.taskexecution.dataflow;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.Map;

public interface ExecutionContext<T extends ExecutionResults> {

  NodeIdentifier getNodeIdentifier();

  void setNodeIdentifier(NodeIdentifier nodeIdentifier);

  Map<NodeIdentifier, T> getInputs();

  void addResults(NodeIdentifier identifier, T result);

}
