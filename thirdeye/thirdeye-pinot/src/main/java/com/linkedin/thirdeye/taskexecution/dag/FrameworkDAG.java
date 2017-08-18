package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Collection;

public interface FrameworkDAG<T extends FrameworkNode> extends DAG<T> {

  T addNode(T node);

  void addEdge(T source, T sink);

  T getNode(NodeIdentifier nodeIdentifier);

  int size();

  Collection<T> getRootNodes();

  Collection<T> getLeafNodes();

  Collection<T> getAllNodes();

}
