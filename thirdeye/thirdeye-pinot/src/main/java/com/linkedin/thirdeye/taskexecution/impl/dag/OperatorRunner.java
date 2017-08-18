package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import com.linkedin.thirdeye.taskexecution.operator.OperatorResult;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class OperatorRunner extends FrameworkNode<OperatorRunner> {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorRunner.class);

  private NodeConfig nodeConfig = new NodeConfig();
  private Class operatorClass;
  private FrameworkNode logicalParentNode;
  private Set<OperatorRunner> incomingOperatorRunners = new HashSet<>();
  // TODO: Change to OperatorResultReader, which could read result from a remote DB or logicalParentNode.
  private ExecutionStatus executionStatus = ExecutionStatus.RUNNING;
  private OperatorResult operatorResult = new OperatorResult();


  public OperatorRunner(NodeIdentifier nodeIdentifier, NodeConfig nodeConfig, Class operatorClass) {
    this(nodeIdentifier, nodeConfig, operatorClass, null);
  }

  public OperatorRunner(NodeIdentifier nodeIdentifier, NodeConfig nodeConfig, Class operatorClass, FrameworkNode logicalParentNode) {
    this.nodeIdentifier = nodeIdentifier;
    this.nodeConfig = nodeConfig;
    this.operatorClass = operatorClass;
    this.logicalParentNode = logicalParentNode;
  }

  @Override
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
    return Collections.emptyList();
  }

  @Override
  public Class getOperatorClass() {
    return operatorClass;
  }

  @Override
  public void setNodeConfig(NodeConfig nodeConfig) {
    this.nodeConfig = nodeConfig;
  }

  @Override
  public NodeConfig getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public ExecutionStatus getExecutionStatus() {
    return executionStatus;
  }

  @Override
  public OperatorResult getOperatorResult() {
    return operatorResult;
  }

  /**
   * Invokes the execution of the operator that is define for the corresponding node in the DAG and returns its node
   * identifier.
   *
   * @return the node identifier of this node (i.e., OperatorRunner).
   */
  @Override
  public NodeIdentifier call() {
    NodeIdentifier identifier = null;
    try {
      identifier = getIdentifier();
      if (identifier == null) {
        throw new IllegalArgumentException("Node identifier cannot be null");
      }
      int numRetry = nodeConfig.numRetryAtError();
      for (int i = 0; i <= numRetry; ++i) {
        try {
          OperatorConfig operatorConfig = convertNodeConfigToOperatorConfig(nodeConfig);
          Operator operator = initializeOperator(operatorClass, operatorConfig);
          OperatorContext operatorContext = prepareInputOperatorContext(incomingOperatorRunners);
          operatorResult = operator.run(operatorContext);
        } catch (Exception e) {
          if (i == numRetry) {
            setFailure(e);
          }
        }
      }
      if (ExecutionStatus.RUNNING.equals(executionStatus)) {
        executionStatus = ExecutionStatus.SUCCESS;
      }
    } catch (Exception e) {
      setFailure(e);
    }
    return identifier;
  }

  @Override
  public FrameworkNode getLogicalParentNode() {
    return logicalParentNode;
  }

  @Override
  public Collection<FrameworkNode> getLogicalChildNode() {
    return Collections.EMPTY_LIST;
  }

  private void setFailure(Exception e) {
    LOG.error("Failed to execute logicalParentNode: {}.", nodeIdentifier, e);
    if (nodeConfig.skipAtFailure()) {
      executionStatus = ExecutionStatus.SKIPPED;
    } else {
      executionStatus = ExecutionStatus.FAILED;
    }
  }

  // TODO: Implement this method
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

  // TODO: Expand this method to consider partitioning
  private OperatorContext prepareInputOperatorContext(Collection<OperatorRunner> incomingNodes) {
    OperatorContext operatorContext = new OperatorContext();
    operatorContext.setNodeIdentifier(nodeIdentifier);
    for (OperatorRunner incomingNode : incomingNodes) {
      operatorContext.addOperatorResult(incomingNode.getIdentifier(), incomingNode.getOperatorResult());
    }
    return operatorContext;
  }
}
