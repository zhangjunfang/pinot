package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Collection;

public interface DAG<T extends Node> {

  /**
   * Adds a node to the DAG.
   *
   * @param node the node to be added to the DAG.
   *
   * @return the node that has been added to the DAG.
   */
  T addNode(T node);

  /**
   * Adds an edge with the given source and sink nodes.
   *
   * @param source the source of the edge.
   * @param sink the sink of the edge.
   */
  void addEdge(T source, T sink);

  /**
   * Returns the node with the given {@link NodeIdentifier}.
   *
   * @param nodeIdentifier the node identifier.
   *
   * @return the node with the given {@link NodeIdentifier}.
   */
  T getNode(NodeIdentifier nodeIdentifier);

  /**
   * Returns the number of nodes in the DAG.
   *
   * @return the number of nodes in the DAG.
   */
  int size();

  /**
   * Returns all nodes that do not have incoming edges.
   *
   * @return all nodes that do not have incoming edges.
   */
  Collection<T> getRootNodes();

  /**
   * Returns all nodes that do not have outgoing edges.
   *
   * @return all nodes that do not have outgoing edges.
   */
  Collection<T> getLeafNodes();

  /**
   * Returns all nodes in the DAG.
   *
   * @return all nodes in the DAG.
   */
  Collection<T> getAllNodes();
}
