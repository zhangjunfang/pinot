package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.AbstractLogicalNode;
import com.linkedin.thirdeye.taskexecution.dag.FrameworkNode;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.operator.OperatorRunner;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

/**
 * LogicalNode considers partitioning of work.
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
  public ExecutionResultsReader getExecutionResultsReader() {
    if (CollectionUtils.isNotEmpty(physicalNodes)) {
      if (physicalNodes.size() == 1) {
        FrameworkNode physicalNode = (FrameworkNode) CollectionUtils.get(physicalNodes, 0);
        return physicalNode.getExecutionResultsReader();
      } else {
        throw new IllegalArgumentException("Multiple partitions are not supported yet.");
      }
    }
    return new InMemoryExecutionResultsReader();
  }

  @Override
  public NodeIdentifier call() throws Exception {
    OperatorRunner runner = new OperatorRunner(nodeIdentifier, nodeConfig, operatorClass);
    physicalNodes.add(runner);

    for (FrameworkNode pNode : this.getIncomingNodes()) {
      runner.addIncomingExecutionResultReader(pNode.getIdentifier(), pNode.getExecutionResultsReader());
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
