package com.linkedin.thirdeye.taskexecution.dag;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ExecutionResultsTest {
  @Test
  public void testAddAndGetResult() {
    final String resultKey = "";
    final String randomKey = "123"; // a random key that does not equal to result key
    ExecutionResults<String, Integer> executionResults = new ExecutionResults<>(new NodeIdentifier(""));
    executionResults.addResult(new ExecutionResult<>(resultKey, 1));

    ExecutionResult returnedResult = executionResults.getResult(resultKey).get(0);
    Assert.assertNotNull(returnedResult);
    Assert.assertEquals(returnedResult.getResult(), 1);

    List<ExecutionResult<String, Integer>> noResult = executionResults.getResult(randomKey);
    Assert.assertNotNull(noResult);
    Assert.assertEquals(noResult.size(), 0);
  }
}
