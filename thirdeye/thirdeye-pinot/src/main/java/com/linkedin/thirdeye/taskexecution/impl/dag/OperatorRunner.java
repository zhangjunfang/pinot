package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dag.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dag.OperatorResult;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class OperatorRunner extends FrameworkNode {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorRunner.class);

  private NodeConfig nodeConfig = new NodeConfig();
  private Class operatorClass;
  private FrameworkNode logicalNode;
  private Map<NodeIdentifier, ExecutionResults> incomingExecutionResultsMap = new HashMap<>();
  // TODO: Change to OperatorResultReader, which could read result from a remote DB or logicalNode.
  private ExecutionStatus executionStatus = ExecutionStatus.RUNNING;
  private OperatorResult operatorResult = new OperatorResult();
  private ExecutionResults executionResults;


  public OperatorRunner(NodeIdentifier nodeIdentifier, NodeConfig nodeConfig, Class operatorClass) {
    this(nodeIdentifier, nodeConfig, operatorClass, null);
  }

  public OperatorRunner(NodeIdentifier nodeIdentifier, NodeConfig nodeConfig, Class operatorClass, FrameworkNode logicalNode) {
    this.nodeIdentifier = nodeIdentifier;
    this.nodeConfig = nodeConfig;
    this.operatorClass = operatorClass;
    this.logicalNode = logicalNode;
    this.executionResults = new ExecutionResults(nodeIdentifier);
  }

  public void addIncomingExecutionResult(NodeIdentifier nodeIdentifier, ExecutionResults executionResults) {
    incomingExecutionResultsMap.put(nodeIdentifier, executionResults);
  }

  public Map<NodeIdentifier, ExecutionResults> getIncomingExecutionResultsMap() {
    return incomingExecutionResultsMap;
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
  public ExecutionResults getExecutionResults() {
    return executionResults;
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
          OperatorContext operatorContext = prepareInputOperatorContext(incomingExecutionResultsMap);
          ExecutionResult operatorResult = operator.run(operatorContext);
          executionResults.addResult(operatorResult);
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
  public FrameworkNode getLogicalNode() {
    return logicalNode;
  }

  @Override
  public Collection<FrameworkNode> getPhysicalNode() {
    return Collections.EMPTY_LIST;
  }

  private void setFailure(Exception e) {
    LOG.error("Failed to execute node: {}.", nodeIdentifier, e);
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
  private OperatorContext prepareInputOperatorContext(Map<NodeIdentifier, ExecutionResults> incomingExecutionResults) {
    Set keys = new HashSet();
    for (Map.Entry<NodeIdentifier, ExecutionResults> nodeResultsEntry : incomingExecutionResults.entrySet()) {
      ExecutionResults executionResults = nodeResultsEntry.getValue();
      keys.addAll(executionResults.keySet());
    }

    OperatorContext operatorContext = new OperatorContext();
    operatorContext.setNodeIdentifier(nodeIdentifier);
    for (Object key : keys) {
      for (Map.Entry<NodeIdentifier, ExecutionResults> nodeResultsEntry : incomingExecutionResults.entrySet()) {
        List<ExecutionResult> resultWithKey = nodeResultsEntry.getValue().getResult(key);
        operatorContext.addResult(nodeResultsEntry.getKey(), resultWithKey);
      }
    }
    return operatorContext;
  }
}
