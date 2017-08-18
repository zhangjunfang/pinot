package com.linkedin.thirdeye.taskexecution.dag;

import com.linkedin.thirdeye.taskexecution.impl.dag.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.impl.dag.NodeConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorResult;
import java.util.concurrent.Callable;

/**
 * Execution Framework Related Methods.
 *
 * @param <T> the type that extends this interface.
 */
public interface FrameworkNode<T extends FrameworkNode<T>> extends Callable<NodeIdentifier>, Node<T> {

  Class getOperatorClass();

  FrameworkNode getLogicalParentNode();

  FrameworkNode getLogicalChildNode();

  void setNodeConfig(NodeConfig nodeConfig);

  NodeConfig getNodeConfig();

  ExecutionStatus getExecutionStatus();

  OperatorResult getOperatorResult();

  @Override
  NodeIdentifier call() throws Exception;
}
