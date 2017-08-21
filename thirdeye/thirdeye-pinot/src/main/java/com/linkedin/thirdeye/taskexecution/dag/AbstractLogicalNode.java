package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractLogicalNode<T extends AbstractLogicalNode> extends FrameworkNode<T> implements Node<T> {

  private Set<T> incomingEdge = new HashSet<>();
  private Set<T> outgoingEdge = new HashSet<>();

  protected AbstractLogicalNode() {
  }

  protected AbstractLogicalNode(NodeIdentifier nodeIdentifier, Class operatorClass) {
    super(nodeIdentifier, operatorClass);
  }

  public void addIncomingNode(T node) {
    incomingEdge.add(node);
  }

  public void addOutgoingNode(T node) {
    outgoingEdge.add(node);
  }

  public Collection<T> getIncomingNodes() {
    return incomingEdge;
  }

  public Collection<T> getOutgoingNodes() {
    return outgoingEdge;
  }
}
