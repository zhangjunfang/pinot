package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;

public class NodeResult<T> {
  private NodeIdentifier nodeIdentifier;

  public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  public NodeIdentifier getNodeIdentifier() {
    return nodeIdentifier;
  }
}
