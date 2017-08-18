package com.linkedin.thirdeye.taskexecution.impl.dag;

import com.linkedin.thirdeye.taskexecution.dag.DAG;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import com.linkedin.thirdeye.taskexecution.operator.OperatorConfig;
import com.linkedin.thirdeye.taskexecution.operator.OperatorContext;
import com.linkedin.thirdeye.taskexecution.operator.OperatorResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DAGExecutorTest {
  private static final Logger LOG = LoggerFactory.getLogger(DAGExecutorTest.class);
  private ExecutorService pool = Executors.newFixedThreadPool(10);

  /**
   * DAG: root
   */
  @Test
  public void testOneNodeExecution() {
    DAG<LogicalNode> dag = new LogicalPlan();
    LogicalNode root = new LogicalNode("root", ExecutionLogOperator.class);
    dag.addNode(root);

    DAGExecutor<LogicalNode> dagExecutor = new DAGExecutor<>(pool);
    DAGConfig dagConfig = new DAGConfig();
    dagConfig.setStopAtFailure(true);
    dagExecutor.execute(dag, dagConfig);

    // Check not null
    OperatorResult executionResult = dagExecutor.getNode(root.getIdentifier()).getOperatorResult();
    Assert.assertNotNull(executionResult);
    Assert.assertNotNull(executionResult.getResult());
    // Check whether execution order is correct
    List<String> result = (List<String>) executionResult.getResult();
    List<String> expectedResult = new ArrayList<String>() {{
      add("root");
    }};
    for (int i = 0; i < expectedResult.size(); i++) {
      Assert.assertEquals(result.get(i), expectedResult.get(i));
    }
  }

  /**
   * DAG: 1 -> 2 -> 3
   */
  @Test
  public void testOneNodeChainExecution() {
    DAG<LogicalNode> dag = new LogicalPlan();
    LogicalNode node1 = new LogicalNode("1", ExecutionLogOperator.class);
    LogicalNode node2 = new LogicalNode("2", ExecutionLogOperator.class);
    LogicalNode node3 = new LogicalNode("3", ExecutionLogOperator.class);
    dag.addEdge(node1, node2);
    dag.addEdge(node2, node3);

    DAGExecutor<LogicalNode> dagExecutor = new DAGExecutor<>(pool);
    dagExecutor.execute(dag, new DAGConfig());

    // Check not null
    OperatorResult executionResult = dagExecutor.getNode(node3.getIdentifier()).getOperatorResult();
    Assert.assertNotNull(executionResult);
    Assert.assertNotNull(executionResult.getResult());
    // Check whether execution order is correct
    List<String> executionLog = (List<String>) executionResult.getResult();

    List<String> expectedResult = new ArrayList<String>() {{
      add("1");
      add("2");
      add("3");
    }};
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(expectedResult);

    Assert.assertTrue(checkLinearizability(expectedResults, executionLog));
  }

  /**
   * DAG:
   *     root1 -> node12 -> leaf1
   *
   *     root2 -> node22 ---> node23 ----> leaf2
   *                     \             /
   *                      \-> node24 -/
   */
  @Test
  public void testTwoNodeChainsExecution() {
    DAG<LogicalNode> dag = new LogicalPlan();
    LogicalNode node11 = new LogicalNode("root1", ExecutionLogOperator.class);
    LogicalNode node12 = new LogicalNode("node12", ExecutionLogOperator.class);
    LogicalNode leaf1 = new LogicalNode("leaf1", ExecutionLogOperator.class);
    dag.addEdge(node11, node12);
    dag.addEdge(node12, leaf1);

    LogicalNode node21 = new LogicalNode("root2", ExecutionLogOperator.class);
    LogicalNode node22 = new LogicalNode("node22", ExecutionLogOperator.class);
    LogicalNode node23 = new LogicalNode("node23", ExecutionLogOperator.class);
    LogicalNode node24 = new LogicalNode("node24", ExecutionLogOperator.class);
    LogicalNode leaf2 = new LogicalNode("leaf2", ExecutionLogJoinOperator.class);
    dag.addEdge(node21, node22);
    dag.addEdge(node22, node23);
    dag.addEdge(node23, leaf2);
    dag.addEdge(node22, node24);
    dag.addEdge(node24, leaf2);

    DAGExecutor<LogicalNode> dagExecutor = new DAGExecutor<>(pool);
    dagExecutor.execute(dag, new DAGConfig());

    // Check path 1
    {
      // Check not null
      OperatorResult executionResult = dagExecutor.getNode(leaf1.getIdentifier()).getOperatorResult();
      Assert.assertNotNull(executionResult);
      Assert.assertNotNull(executionResult.getResult());
      // Check whether execution order is correct
      List<String> executionLog1 = (List<String>) executionResult.getResult();
      List<String> expectedResult = new ArrayList<String>() {{
        add("root1");
        add("node12");
        add("leaf1");
      }};
      List<List<String>> expectedResults = new ArrayList<>();
      expectedResults.add(expectedResult);
      Assert.assertTrue(checkLinearizability(expectedResults, executionLog1));
    }
    // Check path 2
    {
      // Check not null
      OperatorResult executionResult = dagExecutor.getNode(leaf2.getIdentifier()).getOperatorResult();
      Assert.assertNotNull(executionResult);
      Assert.assertNotNull(executionResult.getResult());
      // Check whether execution order is correct
      List<String> executionLog2 = (List<String>) executionResult.getResult();
      List<String> expectedResult1 = new ArrayList<String>() {{
        add("root2");
        add("node22");
        add("node23");
        add("leaf2");
      }};
      List<String> expectedResult2 = new ArrayList<String>() {{
        add("root2");
        add("node22");
        add("node24");
        add("leaf2");
      }};
      List<List<String>> expectedResults = new ArrayList<>();
      expectedResults.add(expectedResult1);
      expectedResults.add(expectedResult2);
      Assert.assertTrue(checkLinearizability(expectedResults, executionLog2));
    }
  }

  /**
   * DAG:
   *           /---------> 12 -------------\
   *         /                              \
   *       /    /---------> 23 ----------\   \
   * root -> 22                           ------> leaf
   *            \-> 24 --> 25 -----> 27 -/
   *                   \         /
   *                    \-> 26 -/
   */
  @Test
  public void testComplexGraphExecution() {
    DAG<LogicalNode> dag = new LogicalPlan();
    LogicalNode root = new LogicalNode("root", ExecutionLogOperator.class);
    LogicalNode leaf = new LogicalNode("leaf", ExecutionLogJoinOperator.class);

    // sub-path 2
    LogicalNode node22 = new LogicalNode("22", ExecutionLogOperator.class);
    LogicalNode node23 = new LogicalNode("23", ExecutionLogOperator.class);
    LogicalNode node24 = new LogicalNode("24", ExecutionLogOperator.class);
    LogicalNode node25 = new LogicalNode("25", ExecutionLogOperator.class);
    LogicalNode node26 = new LogicalNode("26", ExecutionLogOperator.class);
    LogicalNode node27 = new LogicalNode("27", ExecutionLogJoinOperator.class);
    dag.addEdge(root, node22);
    dag.addEdge(node22, node23);
    dag.addEdge(node22, node24);
    dag.addEdge(node24, node25);
    dag.addEdge(node24, node26);
    dag.addEdge(node25, node27);
    dag.addEdge(node26, node27);
    dag.addEdge(node23, leaf);
    dag.addEdge(node27, leaf);

    // sub-path 1
    LogicalNode node12 = new LogicalNode("12", ExecutionLogOperator.class);
    dag.addEdge(root, node12);
    dag.addEdge(node12, leaf);

    DAGExecutor<LogicalNode> dagExecutor = new DAGExecutor<>(pool);
    dagExecutor.execute(dag, new DAGConfig());

    // Check not null
    OperatorResult executionResult = dagExecutor.getNode(leaf.getIdentifier()).getOperatorResult();
    Assert.assertNotNull(executionResult);
    Assert.assertNotNull(executionResult.getResult());
    // Check whether execution order is correct
    List<String> result = (List<String>) executionResult.getResult();
    List<String> expectedOrder1 = new ArrayList<String>() {{
      add("root");
      add("12");
      add("leaf");
    }};
    List<String> expectedOrder2 = new ArrayList<String>() {{
      add("root");
      add("22");
      add("23");
      add("leaf");
    }};
    List<String> expectedOrder3 = new ArrayList<String>() {{
      add("root");
      add("22");
      add("24");
      add("25");
      add("27");
      add("leaf");
    }};
    List<String> expectedOrder4 = new ArrayList<String>() {{
      add("root");
      add("22");
      add("24");
      add("26");
      add("27");
      add("leaf");
    }};
    List<List<String>> expectedOrders = new ArrayList<>();
    expectedOrders.add(expectedOrder1);
    expectedOrders.add(expectedOrder2);
    expectedOrders.add(expectedOrder3);
    expectedOrders.add(expectedOrder4);
    Assert.assertTrue(checkLinearizability(expectedOrders, result));
  }

  /**
   * DAG: 1 -> 2 -> 3
   */
  @Test
  public void testFailedChainExecution() {
    DAG<LogicalNode> dag = new LogicalPlan();
    LogicalNode node1 = new LogicalNode("1", ExecutionLogOperator.class);
    LogicalNode node2 = new LogicalNode("2", FailedOperator.class);
    LogicalNode node3 = new LogicalNode("3", ExecutionLogOperator.class);
    dag.addEdge(node1, node2);
    dag.addEdge(node2, node3);

    DAGConfig dagConfig = new DAGConfig();
    dagConfig.setStopAtFailure(false);
    DAGExecutor<LogicalNode> dagExecutor = new DAGExecutor<>(pool);
    dagExecutor.execute(dag, dagConfig);

    // Check not null
    OperatorResult executionResult = dagExecutor.getNode(node3.getIdentifier()).getOperatorResult();
    Assert.assertNotNull(executionResult);
    Assert.assertNotNull(executionResult.getResult());
    // Check whether execution order is correct
    List<String> executionLog = (List<String>) executionResult.getResult();

    List<String> expectedResult = new ArrayList<String>() {{
      add("3");
    }};
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(expectedResult);

    Assert.assertTrue(checkLinearizability(expectedResults, executionLog));
  }

  /**
   * An operator that appends node name to a list, which is passed in from its incoming nodes.
   */
  static class ExecutionLogOperator implements Operator {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionLogOperator.class);
    private static final Random random = new Random();

    @Override
    public void initialize(OperatorConfig operatorConfig) {
    }

    @Override
    public OperatorResult run(OperatorContext operatorContext) {
      LOG.info("Running node: {}", operatorContext.getNodeIdentifier().getName());
      OperatorResult operatorResult = new OperatorResult();
      Map<NodeIdentifier, OperatorResult> inputs = operatorContext.getInputs();
      List<String> executionLog = new ArrayList<>();
      for (OperatorResult result : inputs.values()) {
        Object pResult = result.getResult();
        if (result.getResult() instanceof List) {
          List<String> list = (List<String>) pResult;
          executionLog.addAll(list);
        }
      }
      executionLog.add(operatorContext.getNodeIdentifier().getName());
      operatorResult.setResult(executionLog);
      return operatorResult;
    }
  }

  /**
   * An operator that joins lists from its incoming nodes and appends node name to the joined list.
   */
  static class ExecutionLogJoinOperator implements Operator {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionLogJoinOperator.class);

    @Override
    public void initialize(OperatorConfig operatorConfig) {
    }

    @Override
    public OperatorResult run(OperatorContext operatorContext) {
      LOG.info("Running node: {}", operatorContext.getNodeIdentifier().getName());
      OperatorResult operatorResult = new OperatorResult();
      Map<NodeIdentifier, OperatorResult> inputs = operatorContext.getInputs();
      List<String> executionLog = new ArrayList<>();
      for (OperatorResult result : inputs.values()) {
        Object pResult = result.getResult();
        if (result.getResult() instanceof List) {
          List<String> list = (List<String>) pResult;
          for (String s : list) {
            if (!executionLog.contains(s)) {
              executionLog.add(s);
            }
          }
        }
      }
      executionLog.add(operatorContext.getNodeIdentifier().getName());
      operatorResult.setResult(executionLog);
      return operatorResult;
    }
  }

  /**
   * An operator that always fails.
   */
  static class FailedOperator implements Operator {
    @Override
    public void initialize(OperatorConfig operatorConfig) {
    }

    @Override
    public OperatorResult run(OperatorContext operatorContext) {
      throw new UnsupportedOperationException("Failed in purpose.");
    }
  }

  /**
   * Utility method to check if the given execution log, which is a partially-ordered execution that is logged as a
   * sequence of execution, satisfies the expected linearizations, which is given by multiple totally-ordered
   * sub-executions.
   *
   * For example, this DAG execution:
   *
   *     root2 -> node22 ---> node23 ----> leaf2
   *                     \             /
   *                      \-> node24 -/
   *
   * contains two totally-ordered sub-executions:
   *     1. root2 -> node22 -> node23 -> leaf2, and
   *     2. root2 -> node22 -> node24 -> leaf2.
   *
   * Therefore, the input (i.e., executionLog) could be:
   *     1. root2 -> node22 -> node23 -> node24 -> leaf2 or
   *     2. root2 -> node22 -> node24 -> node23 -> leaf2 (node24 is swapped with node23).
   * The expected orders are the two totally-ordered sub-executions.
   *
   * This method checks if the execution order in the log contains all sub-execution in the expected orders.
   *
   * @param expectedOrders the expected sub-executions, which are totally-ordered executions.
   * @param executionLog the execution log, which could be a partially-ordered execution.
   *
   * @return true if the execution log satisfies the expected linearizations.
   */
  private boolean checkLinearizability(List<List<String>> expectedOrders, List<String> executionLog) {
    Set<String> checkedNodes = new HashSet<>();
    for (int i = 0; i < expectedOrders.size(); i++) {
      List<String> expectedOrder = expectedOrders.get(i);
      if (expectedOrder.size() > executionLog.size()) {
        throw new IllegalArgumentException("The " + i + "th expected order should be shorter than the execution log.");
      }
      int expectedIdx = 0;
      int logIdx = 0;
      while (expectedIdx < expectedOrder.size() && logIdx < executionLog.size()) {
        if (expectedOrder.get(expectedIdx).equals(executionLog.get(logIdx))) {
          checkedNodes.add(expectedOrder.get(expectedIdx));
          ++expectedIdx;
        }
        ++logIdx;
      }
      if (expectedIdx < expectedOrder.size()-1) {
        LOG.warn("The location {} in the {}th expected order is not found or out of order.", expectedIdx, i);
        LOG.warn("The expected order: {}", expectedOrder.toString());
        ArrayList<String> subExecutionLog = new ArrayList<>();
        for (String s : executionLog) {
          if (expectedOrder.contains(s)) {
            subExecutionLog.add(s);
          }
        }
        LOG.warn("The execution log: {}", subExecutionLog.toString());
        return false;
      }
    }
    if (checkedNodes.size() < executionLog.size()) {
      LOG.warn("Execution log contains more nodes than expected logs.");
      LOG.warn("Num nodes in expected log: {}, execution log: {}", checkedNodes.size(), executionLog.size());
      Iterator<String> ite = executionLog.iterator();
      while(ite.hasNext()) {
        String s = ite.next();
        if (checkedNodes.contains(s)) {
          checkedNodes.remove(s);
          ite.remove();
        }
      }
      LOG.warn("Additional nodes in execution log: {}", executionLog.toString());
      return false;
    }
    return true;
  }

  /// The followings are tests for the method {@link checkLinearizability}.
  @Test(dataProvider = "ExpectedOrders")
  public void testCheckLegalLinearizability(List<List<String>> expectedOrders) {
    List<String> executionLog = new ArrayList<String>() {{
      add("1");
      add("4");
      add("2");
      add("5");
      add("3");
    }};

    Assert.assertTrue(checkLinearizability(expectedOrders, executionLog));
  }

  @Test(dataProvider = "ExpectedOrders")
  public void testCheckIllegalLinearizability(List<List<String>> expectedOrders) {
    List<String> executionLog = new ArrayList<String>() {{
      add("1");
      add("5");
      add("2");
      add("4");
      add("3");
    }};

    Assert.assertFalse(checkLinearizability(expectedOrders, executionLog));
  }

  @Test(dataProvider = "ExpectedOrders")
  public void testCheckNonExistNodeLinearizability(List<List<String>> expectedOrders) {
    List<String> executionLog = new ArrayList<String>() {{
      add("1");
      add("4");
      add("5");
      add("2");
      add("5");
      add("3");
      add("6");
    }};

    Assert.assertFalse(checkLinearizability(expectedOrders, executionLog));
  }

  @DataProvider(name = "ExpectedOrders")
  public static Object[][] expectedOrders() {
    List<List<String>> expectedOrders = new ArrayList<>();
    List<String> expectedOrder1 = new ArrayList<String>() {{
      add("1");
      add("2");
      add("3");
    }};
    List<String> expectedOrder2 = new ArrayList<String>() {{
      add("1");
      add("4");
      add("5");
      add("3");
    }};
    expectedOrders.add(expectedOrder1);
    expectedOrders.add(expectedOrder2);

    return new Object[][] { { expectedOrders } };
  }
}
