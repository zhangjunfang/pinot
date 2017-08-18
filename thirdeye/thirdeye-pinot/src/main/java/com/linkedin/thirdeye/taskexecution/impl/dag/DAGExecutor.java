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
 * An executor that goes through the DAG and submit the nodes, whose parents are finished, to execution service.
 * An executor takes care of only logical execution (control flow). The physical execution is done by NodeRunner,
 * which could be executed on other machines.
 */
public class DAGExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(DAGExecutor.class);
  private ExecutorCompletionService<NodeIdentifier> executorCompletionService;

  // TODO: Persistent the following status to a DB in case of executor unexpectedly dies
  private Set<NodeIdentifier> processedNodes = new HashSet<>();
  private Map<NodeIdentifier, NodeRunner> runningNodes = new HashMap<>();


  public DAGExecutor(ExecutorService executorService) {
    this.executorCompletionService = new ExecutorCompletionService<>(executorService);
  }

  public void execute(DAG dag, DAGConfig dagConfig) {
    Collection<? extends Node> nodes = dag.getRootNodes();
    for (Node node : nodes) {
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
        if (ExecutionStatus.FAILED.equals(executionStatus)) {
          LOG.error("Aborting execution because execution of node {} is failed.", nodeIdentifier.toString());
          abortExecution();
          break;
        }
        processedNodes.add(nodeIdentifier);
        // Search for the next node to execute
        Node node = dag.getNode(nodeIdentifier);
        for (Node outgoingNode : node.getOutgoingNodes()) {
          processNode(outgoingNode, dagConfig);
        }
      } catch (InterruptedException | ExecutionException e) {
        // The implementation of NodeRunner needs to guarantee that this block never happens
        LOG.error("Aborting execution because unexpected error.", e);
        abortExecution();
      }
    }
  }

  private void abortExecution() {
    // TODO: wait all runners are stopped and clean up intermediate data
  }

  private void processNode(Node node, DAGConfig dagConfig) {
    if (!isProcessed(node) && parentsAreProcessed(node)) {
      NodeConfig nodeConfig = dagConfig.getNodeConfig(node.getIdentifier());
      NodeRunner nodeRunner = new NodeRunner(node, nodeConfig);
      for (Node pNode : node.getIncomingNodes()) {
        nodeRunner.addIncomingNode(runningNodes.get(pNode.getIdentifier()));
      }

      LOG.info("Submitting node -- {} -- for execution.", node.getIdentifier().toString());
      executorCompletionService.submit(nodeRunner);
      runningNodes.put(node.getIdentifier(), nodeRunner);
    }
  }

  private boolean isProcessed(Node node) {
    return processedNodes.contains(node.getIdentifier());
  }

  private boolean parentsAreProcessed(Node node) {
    for (Node pNode : node.getIncomingNodes()) {
      if (!processedNodes.contains(pNode.getIdentifier())) {
        return false;
      }
    }
    return true;
  }

  public NodeRunner getNodeRunner(NodeIdentifier nodeIdentifier) {
    return runningNodes.get(nodeIdentifier);
  }
}
