package com.linkedin.thirdeye.taskexecution.operator;

public class OperatorResult<T> {
  //TODO: Change Generic type to a well-defined type
  private T result;

  public T getResult() {
    return result;
  }

  public void setResult(T result) {
    this.result = result;
  }
}
