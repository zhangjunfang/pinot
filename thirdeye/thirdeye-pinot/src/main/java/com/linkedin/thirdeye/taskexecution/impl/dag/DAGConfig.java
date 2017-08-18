package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import java.util.HashMap;
import java.util.Map;

public class DAGConfig {
  private boolean stopAtFailure = true;
  private Map<NodeIdentifier, NodeConfig> nodeConfigs = new HashMap<>();

  public boolean stopAtFailure() {
    return stopAtFailure;
  }

  public void setStopAtFailure(boolean stopAtFailure) {
    this.stopAtFailure = stopAtFailure;
  }

  public NodeConfig getNodeConfig(NodeIdentifier nodeIdentifier) {
    if (nodeConfigs.containsKey(nodeIdentifier)) {
      return nodeConfigs.get(nodeIdentifier);
    } else {
      return new NodeConfig();
    }
  }
}
