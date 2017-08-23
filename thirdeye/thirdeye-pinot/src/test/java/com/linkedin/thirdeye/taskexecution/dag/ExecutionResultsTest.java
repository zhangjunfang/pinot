package com.linkedin.thirdeye.taskexecution.dag;

import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResult;
import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResults;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecutionResultsTest {
  @Test
  public void testAddAndGetResult() {
    final String resultKey = "";
    final String randomKey = "123"; // a random key that does not equal to result key
    ExecutionResults<String, Integer> executionResults = new ExecutionResults<>(new NodeIdentifier(""));
    executionResults.addResult(new ExecutionResult<>(resultKey, 1));

    ExecutionResult returnedResult = executionResults.getResult(resultKey);
    Assert.assertNotNull(returnedResult);
    Assert.assertEquals(returnedResult.getResult(), 1);

    ExecutionResult<String, Integer> noResult = executionResults.getResult(randomKey);
    Assert.assertNull(noResult);
  }
}
