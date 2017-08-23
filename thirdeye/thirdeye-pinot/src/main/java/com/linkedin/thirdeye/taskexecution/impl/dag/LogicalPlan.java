package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.AbstractLogicalDAG;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

public class LogicalPlan extends AbstractLogicalDAG<LogicalNode> {
  private Map<NodeIdentifier, LogicalNode> rootNodes = new HashMap<>();
  private Map<NodeIdentifier, LogicalNode> leafNodes = new HashMap<>();
  private Map<NodeIdentifier, LogicalNode> nodes = new HashMap<>();

  /**
   * Add the given node if it has not been inserted to this DAG and returns the node that has the same {@link
   * NodeIdentifier}.
   *
   * @param node the node to be added.
   *
   * @return the node that is just being added or the existing node that has the same {@link NodeIdentifier}.
   */
  @Override
  public LogicalNode addNode(LogicalNode node) {
    if (node.getIdentifier() == null) {
      throw new IllegalArgumentException("Unable to add a node with null node identifier.");
    }
    return getOrAdd(node);
  }

  /**
   * Add the edge from source to sink nodes. If source or sink has not been inserted to the DAG, they will be inserted
   * to the DAG.
   *
   * @param source the source node of the edge.
   * @param sink   the sink edge of the edge.
   */
  @Override
  public void addEdge(LogicalNode source, LogicalNode sink) {
    source = getOrAdd(source);
    sink = getOrAdd(sink);

    if (!source.equals(sink)) {
      source.addOutgoingNode(sink);
      if (leafNodes.containsKey(source.getIdentifier())) {
        leafNodes.remove(source.getIdentifier());
      }
      sink.addIncomingNode(source);
      if (rootNodes.containsKey(sink.getIdentifier())) {
        rootNodes.remove(sink.getIdentifier());
      }
    }
  }

  @Override
  public LogicalNode getNode(NodeIdentifier nodeIdentifier) {
    return nodes.get(nodeIdentifier);
  }

  /**
   * Returns the given node if it has not been inserted to this DAG; otherwise, returns the previous inserted node,
   * which has the same {@link NodeIdentifier}.
   *
   * @param node the node to get or be added.
   *
   * @return the node with the same {@link NodeIdentifier}.
   */
  private LogicalNode getOrAdd(LogicalNode node) {
    NodeIdentifier nodeIdentifier = node.getIdentifier();
    if (!nodes.containsKey(nodeIdentifier)) {
      nodes.put(nodeIdentifier, node);
      rootNodes.put(nodeIdentifier, node);
      leafNodes.put(nodeIdentifier, node);
      return node;
    } else {
      return nodes.get(nodeIdentifier);
    }
  }

  @Override
  public int size() {
    return nodes.size();
  }

  @Override
  public Collection<LogicalNode> getRootNodes() {
    return new HashSet<>(rootNodes.values());
  }

  @Override
  public Collection<LogicalNode> getLeafNodes() {
    return new HashSet<>(leafNodes.values());
  }

  @Override
  public Collection<LogicalNode> getAllNodes() {
    return new HashSet<>(nodes.values());
  }
}
