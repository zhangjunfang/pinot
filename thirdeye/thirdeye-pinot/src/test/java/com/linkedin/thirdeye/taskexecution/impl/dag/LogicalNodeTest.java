package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dataflow.ExecutionResultsReader;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LogicalNodeTest {
  private LogicalNode node;
  @Test
  public void testCreation() throws Exception {
    node = new LogicalNode("Test", null);
  }

  @Test (dependsOnMethods = "testCreation")
  public void testEmptyLogicalNode() throws Exception {
    Assert.assertEquals(node.getExecutionStatus(), ExecutionStatus.SKIPPED);
    ExecutionResultsReader reader = node.getExecutionResultsReader();
    Assert.assertFalse(reader.hasNext());
    Assert.assertNull(node.getLogicalNode());
    Assert.assertNotNull(node.getPhysicalNode());
    Assert.assertEquals(node.getPhysicalNode().size(), 0);
  }

}
