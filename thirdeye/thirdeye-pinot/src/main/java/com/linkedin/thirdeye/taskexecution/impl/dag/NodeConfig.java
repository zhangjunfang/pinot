package com.linkedin.thirdeye.taskexecution.impl.dag;


public class NodeConfig {

  public int numRetryAtError() {
    return 0;
  }

  public boolean skipAtFailure() {
    return false;
  }
}
