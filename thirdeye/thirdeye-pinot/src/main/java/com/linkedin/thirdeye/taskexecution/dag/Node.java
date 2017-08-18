package com.linkedin.thirdeye.taskexecution.dag;

import java.util.Collection;

public interface Node {

  NodeIdentifier getIdentifier();

  Class getOperatorClass();

  void addIncomingNode(Node node);

  void addOutgoingNode(Node node);

  Collection<? extends Node> getIncomingNodes();

  Collection<? extends Node> getOutgoingNodes();
}
