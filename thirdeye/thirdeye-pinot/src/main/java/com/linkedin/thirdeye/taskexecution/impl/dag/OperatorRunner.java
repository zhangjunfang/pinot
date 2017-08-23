package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OperatorRunner considers multi-threading.
 */
class OperatorRunner extends FrameworkNode {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorRunner.class);

  private NodeConfig nodeConfig = new NodeConfig();
  private Class operatorClass;
  private FrameworkNode logicalNode;
  // TODO: Change to ExecutionResultReader, which could read result from a remote DB or logicalNode.
  private Map<NodeIdentifier, ExecutionResultsReader> incomingResultsReaderMap = new HashMap<>();
  private ExecutionStatus executionStatus = ExecutionStatus.RUNNING;
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

  public void addIncomingExecutionResultReader(NodeIdentifier nodeIdentifier, ExecutionResultsReader executionResultsReader) {
    incomingResultsReaderMap.put(nodeIdentifier, executionResultsReader);
  }

  public Map<NodeIdentifier, ExecutionResultsReader> getIncomingResultsReaderMap() {
    return incomingResultsReaderMap;
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
  public ExecutionResultsReader getExecutionResultsReader() {
    return new InMemoryExecutionResultsReader(executionResults);
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
          OperatorContext operatorContext = buildInputOperatorContext(nodeIdentifier, incomingResultsReaderMap);
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
    return Collections.emptyList();
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

  static Operator initializeOperator(Class operatorClass, OperatorConfig operatorConfig)
      throws IllegalAccessException, InstantiationException {
    try {
      Operator operator = (Operator) operatorClass.newInstance();
      operator.initialize(operatorConfig);
      return operator;
    } catch (Exception e) {
      // We cannot do anything if something bad happens here excepting rethrow the exception.
      LOG.warn("Failed to initialize {}", operatorClass.getName());
      throw e;
    }
  }

  // TODO: Expand this method to consider multi-threading
  static OperatorContext buildInputOperatorContext(NodeIdentifier nodeIdentifier,
      Map<NodeIdentifier, ExecutionResultsReader> incomingResultsReader) {
    // Experimental code for considering multi-threading
//    Set keys = new HashSet();
//    for (Map.Entry<NodeIdentifier, ExecutionResultsReader> nodeResultsEntry : incomingResultsReader.entrySet()) {
//      ExecutionResultsReader resultsReader = nodeResultsEntry.getValue();
//      while (resultsReader.hasNext()) {
//        ExecutionResult next = resultsReader.next();
//        keys.add(next.key());
//      }
//    }

    OperatorContext operatorContext = new OperatorContext();
    operatorContext.setNodeIdentifier(nodeIdentifier);
    for (Map.Entry<NodeIdentifier, ExecutionResultsReader> nodeReadersEntry : incomingResultsReader.entrySet()) {
      ExecutionResults executionResults = new ExecutionResults(nodeReadersEntry.getKey());
      ExecutionResultsReader reader = nodeReadersEntry.getValue();
      boolean hasResult = false;
      while (reader.hasNext()) {
        executionResults.addResult(reader.next());
        hasResult = true;
      }
      // Experimental code for considering multi-threading
//      for (Object key : keys) {
//        executionResults.addResult(reader.get(key));
//      }
      if (hasResult) {
        operatorContext.addResults(nodeReadersEntry.getKey(), executionResults);
      }
    }
    return operatorContext;
  }
}
