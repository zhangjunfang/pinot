package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.DAG;
import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An single machine executor that goes through the DAG and submit the nodes, whose parents are finished, to execution
 * service. An executor takes care of only logical execution (control flow). The physical execution is done by
 * OperatorRunner, which could be executed on other machines.
 */
public class DAGExecutor<T extends Node<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(DAGExecutor.class);
  private ExecutorCompletionService<NodeIdentifier> executorCompletionService;

  // TODO: Persistent the following status to a DB in case of executor unexpectedly dies
  private Set<NodeIdentifier> processedNodes = new HashSet<>();
  private Map<NodeIdentifier, T> runningNodes = new HashMap<>();


  public DAGExecutor(ExecutorService executorService) {
    this.executorCompletionService = new ExecutorCompletionService<>(executorService);
  }

  public void execute(DAG<T> dag, DAGConfig dagConfig) {
    Collection<T> nodes = dag.getRootNodes();
    for (T node : nodes) {
      processNode(node, dagConfig);
    }
    while (runningNodes.size() > processedNodes.size()) {
      try {
        LOG.info("Getting next completed node.");
        NodeIdentifier nodeIdentifier = executorCompletionService.take().get();
        ExecutionStatus executionStatus = runningNodes.get(nodeIdentifier).getExecutionStatus();
        assert (!ExecutionStatus.RUNNING.equals(executionStatus));
        LOG.info("Execution status of node {}: {}.", nodeIdentifier.toString(), executionStatus);
        // Check whether the execution should be stopped upon execution failure
        if (ExecutionStatus.FAILED.equals(executionStatus) && dagConfig.stopAtFailure()) {
          LOG.error("Aborting execution because execution of node {} is failed.", nodeIdentifier.toString());
          abortExecution();
          break;
        }
        processedNodes.add(nodeIdentifier);
        // Search for the next node to execute
        T node = dag.getNode(nodeIdentifier);
        for (T outgoingNode : node.getOutgoingNodes()) {
          processNode(outgoingNode, dagConfig);
        }
      } catch (InterruptedException | ExecutionException e) {
        // The implementation of OperatorRunner needs to guarantee that this block never happens
        LOG.error("Aborting execution because unexpected error.", e);
        abortExecution();
        break;
      }
    }
  }

  private void abortExecution() {
    // TODO: wait all runners are stopped and clean up intermediate data
  }

  private void processNode(T node, DAGConfig dagConfig) {
    if (!isProcessed(node) && parentsAreProcessed(node)) {
      NodeConfig nodeConfig = dagConfig.getNodeConfig(node.getIdentifier());
      if (nodeConfig == null) {
        nodeConfig = new NodeConfig();
      }
      node.setNodeConfig(nodeConfig);

      LOG.info("Submitting node -- {} -- for execution.", node.getIdentifier().toString());
      executorCompletionService.submit(node);
      runningNodes.put(node.getIdentifier(), node);
    }
  }

  private boolean isProcessed(Node node) {
    return processedNodes.contains(node.getIdentifier());
  }

  private boolean parentsAreProcessed(T node) {
    for (Node pNode : node.getIncomingNodes()) {
      if (!processedNodes.contains(pNode.getIdentifier())) {
        return false;
      }
    }
    return true;
  }

  public T getNode(NodeIdentifier nodeIdentifier) {
    return runningNodes.get(nodeIdentifier);
  }
}
