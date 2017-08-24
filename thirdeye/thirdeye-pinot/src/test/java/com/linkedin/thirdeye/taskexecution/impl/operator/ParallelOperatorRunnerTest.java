package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.impl.dag.InMemoryExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.NodeConfig;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParallelOperatorRunnerTest {

  @Test
  public void testCreation() {
    try {
      new ParallelOperatorRunner(new NodeIdentifier(), new NodeConfig(), OperatorRunnerTest.DummyOperator.class);
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
    String key11 = "result1121";
    String key12 = "result12";
    String key21 = "result1121";
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

    List<OperatorContext> operatorContextList = ParallelOperatorRunner
        .buildInputOperatorContext(new NodeIdentifier("OperatorContextBuilder"), incomingResultsReader);

    Assert.assertTrue(CollectionUtils.isNotEmpty(operatorContextList));
    Assert.assertEquals(operatorContextList.size(), 3);

    for (OperatorContext operatorContext : operatorContextList) {
      Map<NodeIdentifier, ExecutionResults> inputs = operatorContext.getInputs();
      Set keySet = new HashSet();
      for (Map.Entry<NodeIdentifier, ExecutionResults> nodeExecutionResultsEntry : inputs.entrySet()) {
        ExecutionResults executionResults = nodeExecutionResultsEntry.getValue();
        keySet.addAll(executionResults.keySet());
      }
      Assert.assertEquals(keySet.size(), 1);
      String key = (String) CollectionUtils.get(keySet, 0);
      ExecutionResults node1ExecutionResults = inputs.get(node1Identifier);
      ExecutionResults node2ExecutionResults = inputs.get(node2Identifier);
      ExecutionResults node3ExecutionResults = inputs.get(node3Identifier);
      switch (key) {
      case "result1121":
        Assert.assertEquals(node1ExecutionResults.getResult(key).result(), 11);
        Assert.assertEquals(node2ExecutionResults.getResult(key).result(), 21);
        Assert.assertNull(node3ExecutionResults.getResult(key));
        break;
      case "result12":
        Assert.assertEquals(node1ExecutionResults.getResult(key).result(), 12);
        Assert.assertNull(node2ExecutionResults.getResult(key));
        Assert.assertNull(node3ExecutionResults.getResult(key));
        break;
      case "result22":
        Assert.assertNull(node1ExecutionResults.getResult(key));
        Assert.assertEquals(node2ExecutionResults.getResult(key).result(), 22);
        Assert.assertNull(node3ExecutionResults.getResult(key));
        break;
      default:
        Assert.fail();
      }
    }
  }

  @Test
  public void testSuccessRunOfSingleOperator() {
    NodeConfig nodeConfig = new NodeConfig();
    nodeConfig.setSkipAtFailure(false);
    nodeConfig.setNumRetryAtError(0);

    ExecutionResults<String, Integer> executionResults = new ExecutionResults<>(new NodeIdentifier("DummyParent"));
    ExecutionResult<String, Integer> executionResult = new ExecutionResult<>("TestDummy1", 123);
    executionResults.addResult(executionResult);
    ExecutionResultsReader reader = new InMemoryExecutionResultsReader(executionResults);

    ParallelOperatorRunner runner =
        new ParallelOperatorRunner(new NodeIdentifier(), nodeConfig, DummyOperator.class);
    runner.addIncomingExecutionResultReader(new NodeIdentifier("DummyNode"), reader);
    runner.call();
    Assert.assertEquals(runner.getExecutionStatus(), ExecutionStatus.SUCCESS);

    ExecutionResultsReader executionResultsReader = runner.getExecutionResultsReader();
    Assert.assertTrue(executionResultsReader.hasNext());

    Assert.assertEquals(executionResultsReader.next().result(), 1);
  }

  @Test
  public void testSuccessRunOfParallelOperators() {
    NodeConfig nodeConfig = new NodeConfig();
    nodeConfig.setSkipAtFailure(false);
    nodeConfig.setNumRetryAtError(0);

    ExecutionResults<String, Integer> n1EexecutionResults = new ExecutionResults<>(new NodeIdentifier("DummyParent1"));
    ExecutionResult<String, Integer> n1ExecutionResult = new ExecutionResult<>("Dimension1", 123);
    n1EexecutionResults.addResult(n1ExecutionResult);
    ExecutionResultsReader node1Reader = new InMemoryExecutionResultsReader(n1EexecutionResults);

    ExecutionResults<String, Integer> n2ExecutionResults = new ExecutionResults<>(new NodeIdentifier("DummyParent2"));
    ExecutionResult<String, Integer> n2ExecutionResult = new ExecutionResult<>("Dimension2", 345);
    n2ExecutionResults.addResult(n2ExecutionResult);
    ExecutionResultsReader node2Reader = new InMemoryExecutionResultsReader(n2ExecutionResults);

    ParallelOperatorRunner runner =
        new ParallelOperatorRunner(new NodeIdentifier(), nodeConfig, DummyOperator.class);
    runner.addIncomingExecutionResultReader(new NodeIdentifier("DummyParent1"), node1Reader);
    runner.addIncomingExecutionResultReader(new NodeIdentifier("DummyParent2"), node2Reader);
    runner.call();
    Assert.assertEquals(runner.getExecutionStatus(), ExecutionStatus.SUCCESS);

    ExecutionResultsReader executionResultsReader = runner.getExecutionResultsReader();
    Assert.assertTrue(executionResultsReader.hasNext());
    int recordCounter = 0;
    while (executionResultsReader.hasNext()) {
      ExecutionResult next = executionResultsReader.next();
      String key = (String) next.key();
      int value = (Integer) next.result();
      switch (key) {
      case "Dimension1":
        Assert.assertEquals(value, 1);
        break;
      case "Dimension2":
        Assert.assertEquals(value, 2);
        break;
      default:
        Assert.fail();
      }
      ++recordCounter;
    }
    Assert.assertEquals(recordCounter, 2);
  }

  public static class DummyOperator implements Operator {
    @Override
    public void initialize(OperatorConfig operatorConfig) {
    }

    @Override
    public ExecutionResult run(OperatorContext operatorContext) {
      Map<NodeIdentifier, ExecutionResults> inputs = operatorContext.getInputs();
      Set keySet = new HashSet();
      for (ExecutionResults executionResults : inputs.values()) {
        keySet.addAll(executionResults.keySet());
      }
      String key = (String) CollectionUtils.get(keySet, 0);
      return new ExecutionResult<String, Integer>(key, Integer.valueOf(key.replaceAll("[^0-9]", "")));
    }
  }

}
