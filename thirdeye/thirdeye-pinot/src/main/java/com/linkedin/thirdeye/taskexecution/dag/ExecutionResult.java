package com.linkedin.thirdeye.taskexecution.dag;

public class ExecutionResult<T> {
  private T result;
  private Class resultClass;

  public T getResult() {
    return result;
  }

  public void setResult(T result) {
    this.result = result;
    if (result != null) {
      resultClass = result.getClass();
    } else {
      resultClass = Object.class;
    }
  }

  public Class getResultClass() {
    return resultClass;
  }
}
