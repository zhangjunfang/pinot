package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.impl.dag.InMemoryExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.NodeConfig;
import com.linkedin.thirdeye.taskexecution.impl.operator.OperatorRunner;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OperatorRunnerTest {

  @Test
  public void testCreation() {
    try {
      new OperatorRunner(new NodeIdentifier(), new NodeConfig(), DummyOperator.class);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testBuildInputOperatorContext() {
    Map<NodeIdentifier, ExecutionResultsReader> incomingResultsReader = new HashMap<>();
    NodeIdentifier node1Identifier = new NodeIdentifier("node1");
    NodeIdentifier node2Identifier = new NodeIdentifier("node2");
    NodeIdentifier node3Identifier = new NodeIdentifier("node3");
    String key11 = "result11";
    String key12 = "result12";
    String key21 = "result21";
    String key22 = "result22";
    {
      ExecutionResults<String, Integer> executionResults1 = new ExecutionResults<>(node1Identifier);
      executionResults1.addResult(new ExecutionResult<>(key11, 11));
      executionResults1.addResult(new ExecutionResult<>(key12, 12));
      ExecutionResultsReader<String, Integer> reader1 = new InMemoryExecutionResultsReader<>(executionResults1);
      incomingResultsReader.put(node1Identifier, reader1);
    }
    {
      ExecutionResults<String, Integer> executionResults2 = new ExecutionResults<>(node2Identifier);
      executionResults2.addResult(new ExecutionResult<>(key21, 21));
      executionResults2.addResult(new ExecutionResult<>(key22, 22));
      ExecutionResultsReader<String, Integer> reader2 = new InMemoryExecutionResultsReader<>(executionResults2);
      incomingResultsReader.put(node2Identifier, reader2);
    }
    {
      ExecutionResults<String, Integer> executionResults3 = new ExecutionResults<>(node3Identifier);
      ExecutionResultsReader<String, Integer> reader3 = new InMemoryExecutionResultsReader<>(executionResults3);
      incomingResultsReader.put(node3Identifier, reader3);
    }

    OperatorContext<String, Integer> operatorContext =
        OperatorRunner.buildInputOperatorContext(new NodeIdentifier("OperatorContextBuilder"), incomingResultsReader);

    Assert.assertEquals(operatorContext.getNodeIdentifier(), new NodeIdentifier("OperatorContextBuilder"));
    Map<NodeIdentifier, ExecutionResults<String, Integer>> inputs = operatorContext.getInputs();
    Assert.assertTrue(MapUtils.isNotEmpty(inputs));
    ExecutionResults<String, Integer> executionResults1 = inputs.get(node1Identifier);
    Assert.assertEquals(executionResults1.keySet().size(), 2);
    Assert.assertEquals(executionResults1.getResult(key11).result(), Integer.valueOf(11));
    Assert.assertEquals(executionResults1.getResult(key12).result(), Integer.valueOf(12));
    ExecutionResults<String, Integer> executionResults2 = inputs.get(node2Identifier);
    Assert.assertEquals(executionResults2.keySet().size(), 2);
    Assert.assertEquals(executionResults2.getResult(key21).result(), Integer.valueOf(21));
    Assert.assertEquals(executionResults2.getResult(key22).result(), Integer.valueOf(22));
    ExecutionResults<String, Integer> executionResults3 = inputs.get(node3Identifier);
    Assert.assertNull(executionResults3);
  }

  @Test
  public void testSuccessRunOfOperator() {
    NodeConfig nodeConfig = new NodeConfig();
    nodeConfig.setSkipAtFailure(false);
    nodeConfig.setNumRetryAtError(0);

    ExecutionResults<String, Integer> executionResults = new ExecutionResults<>(new NodeIdentifier("DummyParent"));
    ExecutionResult<String, Integer> executionResult = new ExecutionResult<>("TestDummy", 123);
    executionResults.addResult(executionResult);
    ExecutionResultsReader reader = new InMemoryExecutionResultsReader(executionResults);

    OperatorRunner runner = new OperatorRunner(new NodeIdentifier(), nodeConfig, DummyOperator.class);
    runner.addIncomingExecutionResultReader(new NodeIdentifier("DummyNode"), reader);
    runner.call();
    Assert.assertEquals(runner.getExecutionStatus(), ExecutionStatus.SUCCESS);

    ExecutionResultsReader executionResultsReader = runner.getExecutionResultsReader();
    Assert.assertTrue(executionResultsReader.hasNext());

    Assert.assertEquals(executionResultsReader.next().result(), 0);
  }

  @Test
  public void testFailureRunOfOperator() {
    NodeConfig nodeConfig = new NodeConfig();
    nodeConfig.setSkipAtFailure(false);
    nodeConfig.setNumRetryAtError(1);
    OperatorRunner runner = new OperatorRunner(new NodeIdentifier(), nodeConfig, FailedRunOperator.class);
    runner.call();
    Assert.assertEquals(runner.getExecutionStatus(), ExecutionStatus.FAILED);
  }

  @Test
  public void testSkippedRunOfOperator() {
    NodeConfig nodeConfig = new NodeConfig();
    nodeConfig.setSkipAtFailure(true);
    nodeConfig.setNumRetryAtError(2);
    OperatorRunner runner = new OperatorRunner(new NodeIdentifier(), nodeConfig, FailedRunOperator.class);
    runner.call();
    Assert.assertEquals(runner.getExecutionStatus(), ExecutionStatus.SKIPPED);
  }

  public static class DummyOperator implements Operator {
    @Override
    public void initialize(OperatorConfig operatorConfig) {
    }

    @Override
    public ExecutionResult run(OperatorContext operatorContext) {
      return new ExecutionResult<String, Integer>("I am a Dummy", 0);
    }
  }

  public static class FailedRunOperator implements Operator {
    @Override
    public void initialize(OperatorConfig operatorConfig) {
    }

    @Override
    public ExecutionResult run(OperatorContext operatorContext) {
      throw new UnsupportedOperationException("Failed during running IN PURPOSE.");
    }
  }
}
