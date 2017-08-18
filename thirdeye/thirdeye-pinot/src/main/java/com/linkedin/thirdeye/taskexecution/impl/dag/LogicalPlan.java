package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.DAG;
import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class LogicalPlan implements DAG {
  private Map<NodeIdentifier, Node> rootNodes = new HashMap<>();
  private Map<NodeIdentifier, Node> leafNodes = new HashMap<>();
  private Map<NodeIdentifier, Node> nodes = new HashMap<>();

  /**
   * Add the given node if it has not been inserted to this DAG and returns the node that has the same {@link
   * NodeIdentifier}.
   *
   * @param node the node to be added.
   *
   * @return the node that is just being added or the existing node that has the same {@link NodeIdentifier}.
   */
  @Override
  public Node addNode(Node node) {
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
  public void addEdge(Node source, Node sink) {
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

  /**
   * Returns the node with the given {@link NodeIdentifier}.
   *
   * @param nodeIdentifier the node identifier.
   *
   * @return the node with the given {@link NodeIdentifier}.
   */
  @Override
  public Node getNode(NodeIdentifier nodeIdentifier) {
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
  private Node getOrAdd(Node node) {
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

  /**
   * Returns the number of nodes in the DAG.
   *
   * @return the number of nodes in the DAG.
   */
  @Override
  public int size() {
    return nodes.size();
  }

  @Override
  public Collection<? extends Node> getRootNodes() {
    return new HashSet<>(rootNodes.values());
  }

  @Override
  public Collection<? extends Node> getLeafNodes() {
    return new HashSet<>(leafNodes.values());
  }

  @Override
  public Collection<? extends Node> getAllNodes() {
    return new HashSet<>(nodes.values());
  }
}
