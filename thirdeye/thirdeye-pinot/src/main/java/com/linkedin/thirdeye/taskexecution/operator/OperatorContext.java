package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.LinkedHashMap;
import java.util.Map;

public class OperatorContext {
  private Node node;
  private Map<NodeIdentifier, OperatorResult> inputs = new LinkedHashMap<>();

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public Map<NodeIdentifier, OperatorResult> getInputs() {
    return inputs;
  }

  public void addOperatorResult(NodeIdentifier identifier, OperatorResult operatorResult) {
    inputs.put(identifier, operatorResult);
  }
}
