package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.AbstractLogicalNode;
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

  private Set<FrameworkNode> physicalNodes = new HashSet<>();


  public LogicalNode(String name, Class operatorClass) {
    super(new NodeIdentifier(name), operatorClass);
  }

  @Override
  public ExecutionStatus getExecutionStatus() {
    // Currently assume that there is only one operator runner
    if (CollectionUtils.isNotEmpty(physicalNodes)) {
      Iterator<FrameworkNode> iterator = physicalNodes.iterator();
      return iterator.next().getExecutionStatus();
    }
    return ExecutionStatus.SKIPPED;
  }

  @Override
  public ExecutionResults getExecutionResults() {
    // Currently assume that there is only one operator runner
    if (CollectionUtils.isNotEmpty(physicalNodes)) {
      FrameworkNode physicalNode = (FrameworkNode) CollectionUtils.get(physicalNodes, 0);
      return physicalNode.getExecutionResults();
    }
    return new ExecutionResults(nodeIdentifier);
  }

  @Override
  public NodeIdentifier call() throws Exception {
    OperatorRunner runner = new OperatorRunner(nodeIdentifier, nodeConfig, operatorClass);
    physicalNodes.add(runner);

    for (FrameworkNode pNode : this.getIncomingNodes()) {
      ExecutionResults executionResults = pNode.getExecutionResults();
      runner.addIncomingExecutionResult(pNode.getIdentifier(), executionResults);
    }
    return runner.call();
  }

  @Override
  public LogicalNode getLogicalNode() {
    return null;
  }

  @Override
  public Collection<FrameworkNode> getPhysicalNode() {
    return physicalNodes;
  }
}
