package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.impl.dag.InMemoryExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.NodeConfig;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ParallelOperatorRunner extends AbstractOperatorRunner {

  private ExecutionResults executionResults;

  public ParallelOperatorRunner(NodeIdentifier nodeIdentifier, NodeConfig nodeConfig, Class operatorClass) {
    this(nodeIdentifier, nodeConfig, operatorClass, null);
  }

  public ParallelOperatorRunner(NodeIdentifier nodeIdentifier, NodeConfig nodeConfig, Class operatorClass,
      FrameworkNode logicalNode) {
    super(nodeIdentifier, nodeConfig, operatorClass, logicalNode);
    this.executionResults = new ExecutionResults(nodeIdentifier);
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
          List<OperatorContext> operatorContexts = buildInputOperatorContext(nodeIdentifier, incomingResultsReaderMap);
          // TODO: Submit each context to an individual thread
          for (OperatorContext operatorContext : operatorContexts) {
            ExecutionResult operatorResult = operator.run(operatorContext);
            // Assume that each operator generates a result with non-duplicated key
            executionResults.addResult(operatorResult);
          }
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

  static List<OperatorContext> buildInputOperatorContext(NodeIdentifier nodeIdentifier,
      Map<NodeIdentifier, ExecutionResultsReader> incomingResultsReader) {
    // Experimental code for considering multi-threading
    Set keys = new HashSet();
    for (Map.Entry<NodeIdentifier, ExecutionResultsReader> nodeResultsEntry : incomingResultsReader.entrySet()) {
      ExecutionResultsReader resultsReader = nodeResultsEntry.getValue();
      while (resultsReader.hasNext()) {
        ExecutionResult next = resultsReader.next();
        keys.add(next.key());
      }
    }

    List<OperatorContext> operatorContexts = new ArrayList<>();
    for (Object key : keys) {
      OperatorContext operatorContext = new OperatorContext();
      operatorContext.setNodeIdentifier(nodeIdentifier);
      for (NodeIdentifier pNodeIdentifier : incomingResultsReader.keySet()) {
        ExecutionResultsReader reader = incomingResultsReader.get(pNodeIdentifier);
        ExecutionResult executionResult = reader.get(key);
        ExecutionResults executionResults = new ExecutionResults(pNodeIdentifier);
        if (executionResult != null) {
          executionResults.addResult(executionResult);
        }
        operatorContext.addResults(pNodeIdentifier, executionResults);
      }
      operatorContexts.add(operatorContext);
    }
    return operatorContexts;
  }
}
