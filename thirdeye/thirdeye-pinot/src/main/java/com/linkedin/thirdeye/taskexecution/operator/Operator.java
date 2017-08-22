package com.linkedin.thirdeye.taskexecution.operator;

import com.linkedin.thirdeye.taskexecution.dag.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dag.OperatorResult;

public interface Operator {

  void initialize(OperatorConfig operatorConfig);

  ExecutionResult run(OperatorContext operatorContext);
}
