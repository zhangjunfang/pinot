package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.LinkedHashMap;
import java.util.Map;

public class OperatorContext {
  private NodeIdentifier nodeIdentifier;
  private Map<NodeIdentifier, OperatorResult> inputs = new LinkedHashMap<>();

  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }

  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  public Map<NodeIdentifier, OperatorResult> getInputs() {
    return inputs;
  }

  public void addOperatorResult(NodeIdentifier identifier, OperatorResult operatorResult) {
    inputs.put(identifier, operatorResult);
  }
}
