package com.linkedin.thirdeye.taskexecution.impl.operator;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.InMemoryExecutionResultsReader;
import com.linkedin.thirdeye.taskexecution.impl.dag.NodeConfig;
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

}
