package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractOperatorRunnerTest {

  @Test
  public void testSuccessInitializeOperator() throws InstantiationException, IllegalAccessException {
    AbstractOperatorRunner.initializeOperator(OperatorRunnerTest.DummyOperator.class, new OperatorConfig());
  }

  @Test
  public void testFailureInitializeOperator() {
    try {
      AbstractOperatorRunner.initializeOperator(FailedInitializedOperator.class, new OperatorConfig());
    } catch (Exception e) {
      return;
    }
    Assert.fail();
  }

  public static class FailedInitializedOperator implements Operator {
    @Override
    public void initialize(OperatorConfig operatorConfig) {
      throw new UnsupportedOperationException("Failed during initialization IN PURPOSE.");
    }

    @Override
    public ExecutionResult run(OperatorContext operatorContext) {
      return new ExecutionResult();
    }
  }

}
