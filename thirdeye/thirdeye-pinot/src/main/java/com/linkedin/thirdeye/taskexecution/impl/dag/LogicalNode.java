package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LogicalNode implements Node {
  private NodeIdentifier nodeIdentifier = new NodeIdentifier();
  private Class operatorClass;
  private Set<LogicalNode> incomingEdge = new HashSet<>();
  private Set<LogicalNode> outgoingEdge = new HashSet<>();


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
  public void addIncomingNode(Node node) {
    addIncomingNode((LogicalNode) node);
  }

  public void addIncomingNode(LogicalNode node) {
    incomingEdge.add(node);
  }

  @Override
  public void addOutgoingNode(Node node) {
    addOutgoingNode((LogicalNode) node);
  }

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
}
