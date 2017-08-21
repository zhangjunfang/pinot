package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Collection;

public interface Node<T extends Node> {

  NodeIdentifier getIdentifier();

  //// Topology Related Methods ////
  void addIncomingNode(T node);

  void addOutgoingNode(T node);

  Collection<T> getIncomingNodes();

  Collection<T> getOutgoingNodes();

}
