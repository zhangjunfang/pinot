package com.linkedin.thirdeye.taskexecution.impl.dag;


public class NodeConfig {
  private int numRetryAtError = 0;
  private boolean skipAtFailure = false;

  public int numRetryAtError() {
    return numRetryAtError;
  }

  public void setNumRetryAtError(int numRetryAtError) {
    this.numRetryAtError = numRetryAtError;
  }

  public boolean skipAtFailure() {
    return skipAtFailure;
  }

  public void setSkipAtFailure(boolean skipAtFailure) {
    this.skipAtFailure = skipAtFailure;
  }
}
