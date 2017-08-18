package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import com.linkedin.thirdeye.taskexecution.operator.OperatorResult;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NodeRunner implements Callable<NodeIdentifier>, Node {
  private static final Logger LOG = LoggerFactory.getLogger(NodeRunner.class);

  private Node node;
  private NodeConfig nodeConfig;
  private OperatorResult operatorResult;
  private ExecutionStatus executionStatus = ExecutionStatus.RUNNING;
  private Set<NodeRunner> incomingNodeRunners = new HashSet<>();


  public NodeRunner(Node node, NodeConfig nodeConfig) {
    this.node = node;
    this.nodeConfig = nodeConfig;
  }

  public Node getNode() {
    return node;
  }

  public NodeIdentifier getIdentifier() {
    return node.getIdentifier();
  }

  @Override
  public Class getOperatorClass() {
    return node.getOperatorClass();
  }

  @Override
  public void addIncomingNode(Node node) {
    addIncomingNode((NodeRunner) node);
  }

  public void addIncomingNode(NodeRunner incomingNodeRunner) {
    incomingNodeRunners.add(incomingNodeRunner);
  }

  @Override
  public void addOutgoingNode(Node node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<NodeRunner> getIncomingNodes() {
    return incomingNodeRunners;
  }

  @Override
  public Collection<NodeRunner> getOutgoingNodes() {
    return null;
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
      int numRety = nodeConfig.numRetryAtError();
      for (int i = 0; i <= numRety; ++i) {
        try {
          OperatorConfig operatorConfig = convertNodeConfigToOperatorConfig(nodeConfig);
          Operator operator = initializeOperator(node.getOperatorClass(), operatorConfig);
          OperatorContext operatorContext = prepareInputOperatorContext(node, incomingNodeRunners);
          operatorResult = operator.run(operatorContext);
        } catch (Exception e) {
          if (i == numRety) {
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
    LOG.error("Failed to execute node: {}.", node.getIdentifier());
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

  private OperatorContext prepareInputOperatorContext(Node currentNode, Collection<NodeRunner> incomingNodes) {
    OperatorContext operatorContext = new OperatorContext();
    operatorContext.setNode(currentNode);
    for (NodeRunner incomingNode : incomingNodes) {
      operatorContext.addOperatorResult(incomingNode.getIdentifier(), incomingNode.getOperatorResult());
    }
    return operatorContext;
  }


}
