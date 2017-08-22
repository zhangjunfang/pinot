package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.ExecutionContext;
import com.linkedin.thirdeye.taskexecution.dag.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.HashMap;
import java.util.Map;

public class OperatorContext implements ExecutionContext<ExecutionResult> {
  private NodeIdentifier nodeIdentifier;
  private Map<NodeIdentifier, ExecutionResult> inputs = new HashMap<>();

  @Override
  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  @Override
  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  @Override
  public Map<NodeIdentifier, ExecutionResult> getInputs() {
    return inputs;
  }

  @Override
  public void addResult(NodeIdentifier identifier, ExecutionResult operatorResult) {
    inputs.put(identifier, operatorResult);
  }
}
