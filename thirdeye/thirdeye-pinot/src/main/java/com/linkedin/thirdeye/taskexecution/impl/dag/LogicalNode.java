package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.operator.OperatorResult;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LogicalNode implements FrameworkNode<LogicalNode> {
  private NodeIdentifier nodeIdentifier = new NodeIdentifier();
  private Class operatorClass;
  private Set<LogicalNode> incomingEdge = new HashSet<>();
  private Set<LogicalNode> outgoingEdge = new HashSet<>();
  private NodeConfig nodeConfig;
  private FrameworkNode logicalChildNode;


  public LogicalNode(String name, Class operatorClass) {
    nodeIdentifier.setName(name);
    this.operatorClass = operatorClass;
  }

  @Override
  public NodeIdentifier getIdentifier() {
    return nodeIdentifier;
  }

  @Override
  public Class getOperatorClass() {
    return operatorClass;
  }

  @Override
  public void addIncomingNode(LogicalNode node) {
    incomingEdge.add(node);;
  }

  @Override
  public void addOutgoingNode(LogicalNode node) {
    outgoingEdge.add(node);
  }

  @Override
  public Collection<LogicalNode> getIncomingNodes() {
    return incomingEdge;
  }

  @Override
  public Collection<LogicalNode> getOutgoingNodes() {
    return outgoingEdge;
  }

  @Override
  public FrameworkNode getLogicalParentNode() {
    return null;
  }

  @Override
  public FrameworkNode getLogicalChildNode() {
    return logicalChildNode;
  }

  public void setNodeConfig(NodeConfig nodeConfig) {
    this.nodeConfig = nodeConfig;
  }

  public NodeConfig getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public ExecutionStatus getExecutionStatus() {
    return logicalChildNode.getExecutionStatus();
  }

  @Override
  public OperatorResult getOperatorResult() {
    return logicalChildNode.getOperatorResult();
  }

  @Override
  public NodeIdentifier call() throws Exception {
    logicalChildNode = new OperatorRunner(this, nodeConfig);

    for (FrameworkNode pNode : this.getIncomingNodes()) {
      logicalChildNode.addIncomingNode(pNode.getLogicalChildNode());
    }
    return logicalChildNode.call();
  }
}
