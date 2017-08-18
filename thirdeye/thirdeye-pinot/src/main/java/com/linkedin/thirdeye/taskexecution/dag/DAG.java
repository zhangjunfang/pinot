package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Collection;

public interface DAG {

  Node addNode(Node node);

  void addEdge(Node source, Node sink);

  Node getNode(NodeIdentifier nodeIdentifier);

  int size();

  Collection<? extends Node> getRootNodes();

  Collection<? extends Node> getLeafNodes();

  Collection<? extends Node> getAllNodes();
}
