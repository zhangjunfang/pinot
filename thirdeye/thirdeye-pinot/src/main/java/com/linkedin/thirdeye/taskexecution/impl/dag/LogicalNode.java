package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.AbstractLogicalNode;
import com.linkedin.thirdeye.taskexecution.dag.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dag.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

/**
 * Implementation of FrameworkNode that uses one thread of a machine to execute {@link OperatorRunner}.
 */
public class LogicalNode extends AbstractLogicalNode<LogicalNode> {

  private Set<FrameworkNode> logicalChildNode = new HashSet<>();


  public LogicalNode(String name, Class operatorClass) {
    super(new NodeIdentifier(name), operatorClass);
  }

  @Override
  public ExecutionStatus getExecutionStatus() {
    // Currently assume that there is only one operator runner
    if (CollectionUtils.isNotEmpty(logicalChildNode)) {
      Iterator<FrameworkNode> iterator = logicalChildNode.iterator();
      return iterator.next().getExecutionStatus();
    }
    return ExecutionStatus.SKIPPED;
  }

  @Override
  public ExecutionResults getExecutionResults() {
    ExecutionResults executionResults = new ExecutionResults();
    // Currently assume that there is only one operator runner
    if (CollectionUtils.isNotEmpty(logicalChildNode)) {
      Iterator<FrameworkNode> operatorRunnerIte = logicalChildNode.iterator();
      ExecutionResults operatorResult = operatorRunnerIte.next().getExecutionResults();
      executionResults.addResults(operatorResult.getResults());
    }
    return executionResults;
  }

  @Override
  public NodeIdentifier call() throws Exception {
    OperatorRunner runner = new OperatorRunner(nodeIdentifier, nodeConfig, operatorClass);
    logicalChildNode.add(runner);

    for (FrameworkNode pNode : this.getIncomingNodes()) {
      Collection<FrameworkNode> incomingNodes = pNode.getPhysicalNode();
      for (FrameworkNode incomingNode : incomingNodes) {
        Collection<ExecutionResult> executionResults = incomingNode.getExecutionResults().getResults();
        if (executionResults.size() > 0) {
          Iterator resultIte =executionResults.iterator();
          runner.addIncomingExecutionResult(incomingNode.getIdentifier(), (ExecutionResult) resultIte.next());
        }
      }
    }
    return runner.call();
  }

  @Override
  public LogicalNode getLogicalNode() {
    return null;
  }

  @Override
  public Collection<FrameworkNode> getPhysicalNode() {
    return logicalChildNode;
  }
}
