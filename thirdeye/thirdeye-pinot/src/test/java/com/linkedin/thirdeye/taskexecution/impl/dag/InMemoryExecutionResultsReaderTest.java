package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import java.util.NoSuchElementException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InMemoryExecutionResultsReaderTest {
  @Test
  public void testCreation() throws Exception {
    new InMemoryExecutionResultsReader();
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void testEmptyReader() throws Exception {
    ExecutionResultsReader reader = new InMemoryExecutionResultsReader();
    Assert.assertFalse(reader.hasNext());
    reader.next();
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void testEmptyResult() throws Exception {
    ExecutionResultsReader reader = new InMemoryExecutionResultsReader(new ExecutionResults(new NodeIdentifier("")));
    Assert.assertFalse(reader.hasNext());
    reader.next();
  }

  @Test
  public void testOneResult() {
    ExecutionResults<String, Integer> executionResults = new ExecutionResults<>(new NodeIdentifier(""));
    executionResults.addResult(new ExecutionResult<>("1", 1));
    ExecutionResultsReader reader = new InMemoryExecutionResultsReader(executionResults);
    Assert.assertTrue(reader.hasNext());
    ExecutionResult next = reader.next();
    Assert.assertEquals(next.result(), 1);
    Assert.assertFalse(reader.hasNext());
    try {
      reader.next();
    } catch (NoSuchElementException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testGet() {
    ExecutionResults<String, Integer> executionResults = new ExecutionResults<>(new NodeIdentifier(""));
    executionResults.addResult(new ExecutionResult<>("1", 1));
    ExecutionResultsReader reader = new InMemoryExecutionResultsReader(executionResults);
    Assert.assertEquals(reader.get("1").result(), 1);
  }

}
