package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import com.linkedin.thirdeye.taskexecution.operator.OperatorResult;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class OperatorRunner implements FrameworkNode<OperatorRunner> {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorRunner.class);

  private FrameworkNode logicalParentNode;
  private NodeConfig nodeConfig;
  // TODO: Change to OperatorResultReader, which could read result from a remote DB or logicalParentNode.
  private OperatorResult operatorResult;
  private ExecutionStatus executionStatus = ExecutionStatus.RUNNING;
  private Set<OperatorRunner> incomingOperatorRunners = new HashSet<>();


  public OperatorRunner(FrameworkNode logicalParentNode, NodeConfig nodeConfig) {
    this.logicalParentNode = logicalParentNode;
    this.nodeConfig = nodeConfig;
  }

  public NodeIdentifier getIdentifier() {
    return logicalParentNode.getIdentifier();
  }

  @Override
  public Class getOperatorClass() {
    return logicalParentNode.getOperatorClass();
  }

  public void addIncomingNode(OperatorRunner incomingOperatorRunner) {
    incomingOperatorRunners.add(incomingOperatorRunner);
  }

  @Override
  public void addOutgoingNode(OperatorRunner node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<OperatorRunner> getIncomingNodes() {
    return incomingOperatorRunners;
  }

  @Override
  public Collection<OperatorRunner> getOutgoingNodes() {
    return null;
  }

  @Override
  public FrameworkNode getLogicalParentNode() {
    return logicalParentNode;
  }

  @Override
  public FrameworkNode getLogicalChildNode() {
    return null;
  }

  @Override
  public void setNodeConfig(NodeConfig nodeConfig) {
    this.nodeConfig = nodeConfig;
  }

  @Override
  public NodeConfig getNodeConfig() {
    return nodeConfig;
  }

  public OperatorResult getOperatorResult() {
    return operatorResult;
  }

  public ExecutionStatus getExecutionStatus() {
    return executionStatus;
  }

  @Override
  public NodeIdentifier call() throws Exception {
    try {
      int numRetry = nodeConfig.numRetryAtError();
      for (int i = 0; i <= numRetry; ++i) {
        try {
          OperatorConfig operatorConfig = convertNodeConfigToOperatorConfig(nodeConfig);
          Operator operator = initializeOperator(logicalParentNode.getOperatorClass(), operatorConfig);
          OperatorContext operatorContext = prepareInputOperatorContext(logicalParentNode, incomingOperatorRunners);
          operatorResult = operator.run(operatorContext);
        } catch (Exception e) {
          if (i == numRetry) {
            setFailure();
          }
        }
      }
      if (ExecutionStatus.RUNNING.equals(executionStatus)) {
        executionStatus = ExecutionStatus.SUCCESS;
      }
    } catch (Exception e) {
      setFailure();
    }
    return getIdentifier();
  }

  private void setFailure() {
    LOG.error("Failed to execute logicalParentNode: {}.", logicalParentNode.getIdentifier());
    operatorResult = new OperatorResult();
    if (nodeConfig.skipAtFailure()) {
      executionStatus = ExecutionStatus.SKIPPED;
    } else {
      executionStatus = ExecutionStatus.FAILED;
    }
  }

  private static OperatorConfig convertNodeConfigToOperatorConfig(NodeConfig nodeConfig) {
    return null;
  }

  private static Operator initializeOperator(Class operatorClass, OperatorConfig operatorConfig)
      throws IllegalAccessException, InstantiationException {
    try {
      Operator operator = (Operator) operatorClass.newInstance();
      operator.initialize(operatorConfig);
      return operator;
    } catch (InstantiationException | IllegalAccessException e) {
      LOG.warn("Failed to initialize {}", operatorClass.getName());
      throw e;
    }
  }

  private OperatorContext prepareInputOperatorContext(Node currentNode, Collection<OperatorRunner> incomingNodes) {
    OperatorContext operatorContext = new OperatorContext();
    operatorContext.setNode(currentNode);
    for (OperatorRunner incomingNode : incomingNodes) {
      operatorContext.addOperatorResult(incomingNode.getIdentifier(), incomingNode.getOperatorResult());
    }
    return operatorContext;
  }
}
