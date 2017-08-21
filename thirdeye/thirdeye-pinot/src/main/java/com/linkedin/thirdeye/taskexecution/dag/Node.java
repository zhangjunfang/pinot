package com.linkedin.thirdeye.taskexecution.dag;

import com.linkedin.thirdeye.taskexecution.impl.dag.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.impl.dag.NodeConfig;
import java.util.Collection;
import java.util.concurrent.Callable;

public interface Node<T extends Node> extends Callable<NodeIdentifier> {

  NodeIdentifier getIdentifier();

  //// Topology Related Methods ////
  void addIncomingNode(T node);

  void addOutgoingNode(T node);

  Collection<T> getIncomingNodes();

  Collection<T> getOutgoingNodes();


  //// Execution Related Methods ////
  Class getOperatorClass();

  void setNodeConfig(NodeConfig nodeConfig);

  NodeConfig getNodeConfig();

  ExecutionStatus getExecutionStatus();

  ExecutionResults getExecutionResults();

  @Override
  NodeIdentifier call() throws Exception;
}
