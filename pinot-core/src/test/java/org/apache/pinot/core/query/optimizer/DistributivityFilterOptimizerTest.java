/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Structural (plan-level) tests for the
 * {@link org.apache.pinot.core.query.optimizer.filter.DistributivityFilterOptimizer} (the {@code undistributeANDOverOR}
 * law). Each test runs the full {@link QueryOptimizer} chain on a query and asserts on the rewritten filter tree.
 *
 * <p>Contains the dedicated scenario tests, the {@link #distributivityPermutations()} data-driven permutation matrix,
 * and a disabled compile-path overhead measurement. Execution-level (result-equivalence) tests live separately in
 * {@code DistributivityFilterOptimizerQueriesTest} (single-value) and
 * {@code DistributivityFilterOptimizerMvQueriesTest} (multi-value).
 */
public class DistributivityFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("int", DataType.INT)
          .addSingleValueDimension("long", DataType.LONG).addSingleValueDimension("float", DataType.FLOAT)
          .addSingleValueDimension("double", DataType.DOUBLE).addSingleValueDimension("string", DataType.STRING)
          .addSingleValueDimension("bytes", DataType.BYTES).addMultiValueDimension("mvInt", DataType.INT).build();

  /**
   * Original:  (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR (int = 5 AND long = 3)
   * Rewritten: int = 5 AND long IN (1, 2, 3)
   */
  @Test
  public void testFactorCommonPredicateOutOfOr() {
    String query =
        "SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR (int = 5 AND long = 3)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 2);
    assertEquals(children.get(0), getEqFilterExpression("int", 5));
    checkInFilterFunction(children.get(1).getFunctionCall(), "long", Arrays.asList(1, 2, 3));
  }

  /**
   * Original:  (int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = 'foo' AND long = 2)
   * Rewritten: int = 5 AND string = 'foo' AND long IN (1, 2)
   */
  @Test
  public void testFactorCommonPredicateOutOfOrMultipleCommon() {
    String query = "SELECT * FROM testTable WHERE (int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = "
        + "'foo' AND long = 2)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 3);
    Set<Expression> expectedCommon = new HashSet<>();
    expectedCommon.add(getEqFilterExpression("int", 5));
    expectedCommon.add(getEqFilterExpression("string", "foo"));
    assertTrue(expectedCommon.contains(children.get(0)));
    assertTrue(expectedCommon.contains(children.get(1)));
    checkInFilterFunction(children.get(2).getFunctionCall(), "long", Arrays.asList(1, 2));
  }

  /**
   * Original:  (int = 5 AND long = 1) OR int = 5
   * Rewritten: int = 5            (the branch that is only the common conjunct absorbs the OR)
   */
  @Test
  public void testFactorCommonPredicateAbsorption() {
    String query = "SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR int = 5";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    assertEquals(pinotQuery.getFilterExpression(), getEqFilterExpression("int", 5));
  }

  /**
   * Original:  (int = 1 AND long = 2) OR (int = 3 AND long = 4)
   * Rewritten: (int = 1 AND long = 2) OR (int = 3 AND long = 4)   (unchanged: no conjunct common to all branches)
   */
  @Test
  public void testFactorCommonPredicateNoCommonConjunct() {
    String query = "SELECT * FROM testTable WHERE (int = 1 AND long = 2) OR (int = 3 AND long = 4)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 2);
    for (Expression child : children) {
      assertEquals(child.getFunctionCall().getOperator(), FilterKind.AND.name());
    }
  }

  /**
   * Original:  (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double = 9
   * Rewritten: (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double = 9   (unchanged)
   *
   * <p>Boundary case: int = 5 is common to the first two branches but not to the third (double = 9), so there is no
   * conjunct common to *every* branch. This optimizer only factors a globally-common conjunct (matching DuckDB and
   * ClickHouse), so it leaves the OR untouched here. Partial / grouped factoring —
   * {@code (A AND B) OR (A AND C) OR D -> (A AND (B OR C)) OR D} — is a deferred future enhancement.
   */
  @Test
  public void testFactorCommonPredicatePartialCommonNotFactored() {
    String query = "SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double = 9";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    // Still a 3-way OR; no factoring happened.
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 3);
    // int = 5 must NOT have been factored out to a sibling of the OR (it is not common to the double = 9 branch).
    assertTrue(!children.contains(getEqFilterExpression("int", 5)),
        "int = 5 must not be factored out when it is not common to every OR branch");
    // The two AND-of-EQ branches are preserved as ANDs.
    int numAndBranches = 0;
    for (Expression child : children) {
      if (child.getFunctionCall().getOperator().equals(FilterKind.AND.name())) {
        numAndBranches++;
      }
    }
    assertEquals(numAndBranches, 2);
  }

  /**
   * Original:  (mvInt = 1 AND long = 2) OR (mvInt = 1 AND long = 3)   (mvInt is a multi-value column)
   * Rewritten: mvInt = 1 AND long IN (2, 3)
   *
   * <p>The rewrite is a pure boolean restructuring, so it is correct for multi-value columns and needs no
   * single-value gating.
   */
  @Test
  public void testFactorCommonPredicateMultiValueColumn() {
    String query = "SELECT * FROM testTable WHERE (mvInt = 1 AND long = 2) OR (mvInt = 1 AND long = 3)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 2);
    assertEquals(children.get(0), getEqFilterExpression("mvInt", 1));
    checkInFilterFunction(children.get(1).getFunctionCall(), "long", Arrays.asList(2, 3));
  }

  /**
   * Deeply-nested machine-generated shape (the one that motivated this optimizer): the wide OR-of-ANDs is one conjunct
   * several levels down inside a top-level AND, and every branch shares the conjunct int = 5.
   *
   * Original:  double >= 10 AND double <= 20 AND long IN (1000..1300) AND int > 0 AND long > 0
   *              AND ( (int = 5 AND long = 1000) OR (int = 5 AND long = 1001) OR ... OR (int = 5 AND long = 1099) )
   * Rewritten: double >= 10 AND double <= 20 AND long IN (1000..1300) AND int > 0 AND long > 0
   *              AND int = 5 AND long IN (1000..1099)
   *
   * <p>Note: this optimizer only factors the OR and merges the residual into an IN; it does not remove the
   * now-redundant superset {@code long IN (1000..1300)} or the {@code > 0} predicates (that subsumption is a separate
   * optimizer).
   */
  @Test
  public void testFactorCommonPredicateDeepOriginalShape() {
    int numOrBranches = 100;
    StringBuilder orBlock = new StringBuilder();
    List<Object> residualCampaignIds = new ArrayList<>(numOrBranches);
    for (int i = 0; i < numOrBranches; i++) {
      int campaignId = 1000 + i;
      residualCampaignIds.add(campaignId);
      if (i > 0) {
        orBlock.append(" OR ");
      }
      orBlock.append("(int = 5 AND long = ").append(campaignId).append(")");
    }
    StringBuilder inList = new StringBuilder();
    for (int v = 1000; v <= 1300; v++) {
      if (v > 1000) {
        inList.append(", ");
      }
      inList.append(v);
    }
    String query = "SELECT * FROM testTable WHERE double >= 10 AND double <= 20 AND long IN (" + inList + ") "
        + "AND int > 0 AND long > 0 AND (" + orBlock + ")";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Expression filter = pinotQuery.getFilterExpression();
    Function filterFunction = filter.getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    // The wide OR-of-ANDs must be completely eliminated: no OR node should remain anywhere in the tree.
    assertNoOperator(filter, FilterKind.OR);
    // The common conjunct (int = 5) must be factored up to the top-level AND.
    assertTrue(filterFunction.getOperands().contains(getEqFilterExpression("int", 5)),
        "Expected the common conjunct int = 5 to be factored out to the top-level AND");
    // The residual campaignId equalities must be merged into a single IN predicate with exactly the 100 branch values.
    Function residualIn = findInFilterOnColumn(filterFunction.getOperands(), "long", residualCampaignIds.size());
    assertTrue(residualIn != null, "Expected a merged IN predicate on 'long' with the 100 residual campaignId values");
    checkInFilterFunction(residualIn, "long", residualCampaignIds);
  }

  /**
   * Recursively asserts that the given filter expression contains no function node with the given operator.
   */
  private static void assertNoOperator(Expression expression, FilterKind operator) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    assertNotEquals(function.getOperator(), operator.name(),
        "Did not expect any " + operator.name() + " node to remain in the optimized filter");
    for (Expression operand : function.getOperands()) {
      assertNoOperator(operand, operator);
    }
  }

  /**
   * Returns the IN filter function on the given column with the given number of values from the list of top-level
   * conjuncts, or {@code null} if none matches.
   */
  private static Function findInFilterOnColumn(List<Expression> conjuncts, String column, int numValues) {
    for (Expression conjunct : conjuncts) {
      Function function = conjunct.getFunctionCall();
      if (function != null && function.getOperator().equals(FilterKind.IN.name())
          && function.getOperands().get(0).getIdentifier().getName().equals(column)
          && function.getOperands().size() == numValues + 1) {
        return function;
      }
    }
    return null;
  }


  private static final String TABLE = "SELECT * FROM testTable WHERE ";

  /**
   * Each row is {@code {name, inputFilterSql, expectedFilterSqlOrNull}}. A {@code null} expected value means the input
   * must be left unchanged by the optimizer (a no-op / boundary case).
   */
  @DataProvider(name = "distributivityPermutations")
  public Object[][] distributivityPermutations() {
    List<Object[]> cases = new ArrayList<>();

    // A. Common-conjunct presence.
    cases.add(row("A1 all-share, 2 branches",
        "(int = 1 AND long = 1) OR (int = 1 AND long = 2)",
        "int = 1 AND long IN (1, 2)"));
    cases.add(row("A2 all-share, 3 branches",
        "(int = 1 AND long = 1) OR (int = 1 AND long = 2) OR (int = 1 AND long = 3)",
        "int = 1 AND long IN (1, 2, 3)"));
    cases.add(row("A3 partial-share (one stray branch) -> unchanged",
        "(int = 1 AND long = 1) OR (int = 1 AND long = 2) OR double = 9", null));
    cases.add(row("A4 none-share -> unchanged",
        "(int = 1 AND long = 2) OR (int = 3 AND long = 4)", null));

    // B. Common-conjunct position within each AND branch (order independence).
    cases.add(row("B1 common first",
        "(int = 5 AND long = 1) OR (int = 5 AND long = 2)",
        "int = 5 AND long IN (1, 2)"));
    cases.add(row("B2 common last",
        "(long = 1 AND int = 5) OR (long = 2 AND int = 5)",
        "int = 5 AND long IN (1, 2)"));
    cases.add(row("B3 common at mixed positions",
        "(long = 1 AND int = 5) OR (int = 5 AND long = 2)",
        "int = 5 AND long IN (1, 2)"));

    // C. Number of common conjuncts.
    cases.add(row("C2 two common conjuncts",
        "(int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = 'foo' AND long = 2)",
        "int = 5 AND string = 'foo' AND long IN (1, 2)"));
    cases.add(row("C3 three common conjuncts",
        "(int = 5 AND string = 'foo' AND float = 1.5 AND long = 1) OR (int = 5 AND string = 'foo' AND float = 1.5 AND "
            + "long = 2)",
        "int = 5 AND string = 'foo' AND float = 1.5 AND long IN (1, 2)"));

    // D. Common-conjunct predicate kind.
    cases.add(row("D2 common IN predicate",
        "(int IN (7, 8) AND long = 1) OR (int IN (7, 8) AND long = 2)",
        "int IN (7, 8) AND long IN (1, 2)"));
    cases.add(row("D3 common range predicate (>)",
        "(int > 5 AND long = 1) OR (int > 5 AND long = 2)",
        "int > 5 AND long IN (1, 2)"));
    cases.add(row("D4 common BETWEEN predicate",
        "(int BETWEEN 5 AND 10 AND long = 1) OR (int BETWEEN 5 AND 10 AND long = 2)",
        "int BETWEEN 5 AND 10 AND long IN (1, 2)"));

    // E. Residual predicate kind.
    cases.add(row("E2 residual on different columns stays an OR",
        "(int = 5 AND long = 1) OR (int = 5 AND float = 2.5)",
        "int = 5 AND (long = 1 OR float = 2.5)"));
    cases.add(row("E3 residual ranges stay an OR",
        "(int = 5 AND long > 1) OR (int = 5 AND long < 0)",
        "int = 5 AND (long > 1 OR long < 0)"));

    // F. Residual size.
    cases.add(row("F2 multi-conjunct residuals",
        "(int = 5 AND long = 1 AND float = 1.5) OR (int = 5 AND long = 2 AND float = 2.5)",
        "int = 5 AND ((long = 1 AND float = 1.5) OR (long = 2 AND float = 2.5))"));

    // G. Single- vs multi-value columns.
    cases.add(row("G2 common conjunct on a multi-value column",
        "(mvInt = 1 AND long = 2) OR (mvInt = 1 AND long = 3)",
        "mvInt = 1 AND long IN (2, 3)"));
    cases.add(row("G3 residual on a multi-value column",
        "(int = 5 AND mvInt = 1) OR (int = 5 AND mvInt = 2)",
        "int = 5 AND mvInt IN (1, 2)"));

    // H. Absorption.
    cases.add(row("H1 a branch equal to the common conjunct absorbs the OR",
        "(int = 5 AND long = 1) OR int = 5", "int = 5"));
    cases.add(row("H2 all-identical branches",
        "(int = 5 AND long = 1) OR (int = 5 AND long = 1)", "int = 5 AND long = 1"));
    cases.add(row("H3 absorption with multiple common conjuncts",
        "(int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = 'foo')",
        "int = 5 AND string = 'foo'"));

    // I. Nesting.
    cases.add(row("I1 OR-of-ANDs nested inside a top-level AND",
        "double = 9 AND ((int = 5 AND long = 1) OR (int = 5 AND long = 2))",
        "double = 9 AND int = 5 AND long IN (1, 2)"));
    cases.add(row("I2 OR-of-ANDs under a NOT (residual OR not merged to IN)",
        "NOT ((int = 5 AND long = 1) OR (int = 5 AND long = 2))",
        "NOT (int = 5 AND (long = 1 OR long = 2))"));
    cases.add(row("I3 factorable OR alongside another top-level predicate",
        "int > 0 AND ((int = 5 AND long = 1) OR (int = 5 AND long = 2))",
        "int > 0 AND int = 5 AND long IN (1, 2)"));

    // J. Duplicates.
    cases.add(row("J2 duplicate branch plus a distinct branch",
        "(int = 5 AND long = 1) OR (int = 5 AND long = 1) OR (int = 5 AND long = 2)",
        "int = 5 AND long IN (1, 2)"));

    // K. Scale (generated).
    cases.add(row("K1 factorable OR with 50 branches", generateFactorableOr(50), generateFactoredIn(50)));

    // L. No-op / boundary.
    cases.add(row("L1 single predicate -> unchanged", "int = 5", null));
    cases.add(row("L4 plain OR of equalities (handled by MergeEqIn, distributivity is a no-op)",
        "int = 1 OR int = 2 OR int = 3", "int IN (1, 2, 3)"));

    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "distributivityPermutations")
  public void testDistributivityPermutation(String name, String inputFilter, String expectedFilter) {
    String inputQuery = TABLE + inputFilter;
    if (expectedFilter == null) {
      testCannotOptimizeQuery(inputQuery);
    } else {
      testQuery(inputQuery, TABLE + expectedFilter);
    }
  }

  private static Object[] row(String name, String inputFilter, String expectedFilter) {
    return new Object[]{name, inputFilter, expectedFilter};
  }

  private static String generateFactorableOr(int numBranches) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("(int = 5 AND long = ").append(1000 + i).append(')');
    }
    return sb.toString();
  }

  private static String generateFactoredIn(int numBranches) {
    StringBuilder sb = new StringBuilder("int = 5 AND long IN (");
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(1000 + i);
    }
    return sb.append(')').toString();
  }

  @Test(enabled = false)
  public void testOptimizerOverhead() {
    Map<String, String> shapes = new LinkedHashMap<>();
    shapes.put("point (no OR)", "SELECT * FROM testTable WHERE int = 5 AND long > 0");
    shapes.put("factorable OR x100", buildFactorableOrOfAnds(100));
    shapes.put("non-factorable OR x100", buildNonFactorableOrOfAnds(100));
    shapes.put("original shape (IN + OR x100)", buildOriginalShape(100));

    int warmupIterations = 2000;
    int measuredIterations = 5000;
    long accumulator = 0;

    System.out.println("=== QueryOptimizer per-pass overhead (deep-copy + optimize, minus deep-copy) ===");
    for (Map.Entry<String, String> shape : shapes.entrySet()) {
      PinotQuery base = CalciteSqlParser.compileToPinotQuery(shape.getValue());

      // Warmup to let the JIT compile the hot path.
      for (int i = 0; i < warmupIterations; i++) {
        PinotQuery copy = new PinotQuery(base);
        OPTIMIZER.optimize(copy, SCHEMA);
        accumulator += copy.getFilterExpression().getFunctionCall().getOperandsSize();
      }

      // Measure deep-copy only (the baseline we subtract out).
      long copyOnlyNanos = System.nanoTime();
      for (int i = 0; i < measuredIterations; i++) {
        PinotQuery copy = new PinotQuery(base);
        accumulator += copy.getFilterExpression().getFunctionCall().getOperandsSize();
      }
      copyOnlyNanos = System.nanoTime() - copyOnlyNanos;

      // Measure deep-copy + optimize.
      long copyAndOptimizeNanos = System.nanoTime();
      for (int i = 0; i < measuredIterations; i++) {
        PinotQuery copy = new PinotQuery(base);
        OPTIMIZER.optimize(copy, SCHEMA);
        accumulator += copy.getFilterExpression().getFunctionCall().getOperandsSize();
      }
      copyAndOptimizeNanos = System.nanoTime() - copyAndOptimizeNanos;

      double perPassMicros = Math.max(0, copyAndOptimizeNanos - copyOnlyNanos) / 1000.0 / measuredIterations;
      System.out.printf("  %-32s %8.3f us/pass%n", shape.getKey(), perPassMicros);
      // Loose guard: a single optimize pass must stay well under a millisecond even on a slow machine.
      assertTrue(perPassMicros < 1000.0,
          "Optimizer pass for shape '" + shape.getKey() + "' took " + perPassMicros + " us, unexpectedly high");
    }
    // Prevent dead-code elimination of the optimize() calls.
    assertTrue(accumulator > 0);
  }

  private static String buildFactorableOrOfAnds(int numBranches) {
    StringBuilder sb = new StringBuilder("SELECT * FROM testTable WHERE ");
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("(int = 5 AND long = ").append(1000 + i).append(')');
    }
    return sb.toString();
  }

  private static String buildNonFactorableOrOfAnds(int numBranches) {
    StringBuilder sb = new StringBuilder("SELECT * FROM testTable WHERE ");
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      // No conjunct is common across branches (int value varies per branch).
      sb.append("(int = ").append(i).append(" AND long = ").append(1000 + i).append(')');
    }
    return sb.toString();
  }

  private static String buildOriginalShape(int numBranches) {
    StringBuilder inList = new StringBuilder();
    StringBuilder orBlock = new StringBuilder();
    for (int i = 0; i < numBranches; i++) {
      long campaignId = 1000 + i;
      if (i > 0) {
        inList.append(", ");
        orBlock.append(" OR ");
      }
      inList.append(campaignId);
      orBlock.append("(int = 5 AND long = ").append(campaignId).append(')');
    }
    return "SELECT * FROM testTable WHERE double >= 10 AND double <= 20 AND long IN (" + inList + ") "
        + "AND int > 0 AND long > 0 AND (" + orBlock + ")";
  }

  private static Expression getEqFilterExpression(String column, Object value) {
    return RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), RequestUtils.getIdentifierExpression(column),
        RequestUtils.getLiteralExpression(value));
  }


  private static void checkInFilterFunction(Function inFilterFunction, String column, List<Object> values) {
    assertEquals(inFilterFunction.getOperator(), FilterKind.IN.name());
    List<Expression> operands = inFilterFunction.getOperands();
    int numOperands = operands.size();
    assertEquals(numOperands, values.size() + 1);
    assertEquals(operands.get(0).getIdentifier().getName(), column);
    Set<Expression> valueExpressions = new HashSet<>();
    for (Object value : values) {
      valueExpressions.add(RequestUtils.getLiteralExpression(value));
    }
    for (int i = 1; i < numOperands; i++) {
      assertTrue(valueExpressions.contains(operands.get(i)));
    }
  }

  private static void testQuery(String actual, String expected) {
    assertNotEquals(actual, expected, "You must provide different queries to test");
    PinotQuery actualPinotQuery = CalciteSqlParser.compileToPinotQuery(actual);
    OPTIMIZER.optimize(actualPinotQuery, SCHEMA);
    // Also optimize the expected query because the expected range can only be generate via optimizer
    PinotQuery expectedPinotQuery = CalciteSqlParser.compileToPinotQuery(expected);
    OPTIMIZER.optimize(expectedPinotQuery, SCHEMA);
    comparePinotQuery(actualPinotQuery, expectedPinotQuery);
  }

  private static void testCannotOptimizeQuery(String query) {
    PinotQuery actualPinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(actualPinotQuery, SCHEMA);
    PinotQuery expectedPinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    comparePinotQuery(actualPinotQuery, expectedPinotQuery);
  }

  private static void comparePinotQuery(PinotQuery actual, PinotQuery expected) {
    if (expected.getFilterExpression() == null) {
      assertNull(actual.getFilterExpression());
      return;
    }
    compareFilterExpression(actual.getFilterExpression(), expected.getFilterExpression());
  }

  private static void compareFilterExpression(Expression actual, Expression expected) {
    if (actual.isSetLiteral()) {
      assertNull(actual.getFunctionCall());
      assertNull(expected.getFunctionCall());
      assertTrue(expected.isSetLiteral());
      assertEquals(actual.getLiteral(), expected.getLiteral());
    } else {
      Function actualFilterFunction = actual.getFunctionCall();
      Function expectedFilterFunction = expected.getFunctionCall();
      FilterKind actualFilterKind = FilterKind.valueOf(actualFilterFunction.getOperator());
      FilterKind expectedFilterKind = FilterKind.valueOf(expectedFilterFunction.getOperator());
      List<Expression> actualOperands = actualFilterFunction.getOperands();
      List<Expression> expectedOperands = expectedFilterFunction.getOperands();
      if (!actualFilterKind.isRange()) {
        assertEquals(actualFilterKind, expectedFilterKind);
        assertEquals(actualOperands.size(), expectedOperands.size());
        if (actualFilterKind == FilterKind.AND || actualFilterKind == FilterKind.OR) {
          compareFilterExpressionChildren(actualOperands, expectedOperands);
        } else {
          assertEquals(actualOperands.get(0), expectedOperands.get(0));
          if (actualFilterKind == FilterKind.IN || actualFilterKind == FilterKind.NOT_IN) {
            // Handle different order of values
            assertEqualsNoOrder(actualOperands.toArray(), expectedOperands.toArray());
          } else {
            assertEquals(actualOperands, expectedOperands);
          }
        }
      } else {
        assertTrue(expectedFilterKind.isRange());
        assertEquals(getRangeString(actualFilterKind, actualOperands),
            getRangeString(expectedFilterKind, expectedOperands));
      }
    }
  }

  /**
   * Handles different order of children under AND/OR filter.
   */
  private static void compareFilterExpressionChildren(List<Expression> actual, List<Expression> expected) {
    assertEquals(actual.size(), expected.size());
    List<Expression> unmatchedExpectedChildren = new ArrayList<>(expected);
    for (Expression actualChild : actual) {
      Iterator<Expression> iterator = unmatchedExpectedChildren.iterator();
      boolean findMatchingChild = false;
      while (iterator.hasNext()) {
        try {
          compareFilterExpression(actualChild, iterator.next());
          iterator.remove();
          findMatchingChild = true;
          break;
        } catch (AssertionError e) {
          // Ignore
        }
      }
      if (!findMatchingChild) {
        fail("Failed to find matching child");
      }
    }
  }

  private static String getRangeString(FilterKind filterKind, List<Expression> operands) {
    switch (filterKind) {
      case GREATER_THAN:
        return Range.LOWER_EXCLUSIVE + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_UNBOUNDED;
      case GREATER_THAN_OR_EQUAL:
        return Range.LOWER_INCLUSIVE + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_UNBOUNDED;
      case LESS_THAN:
        return Range.LOWER_UNBOUNDED + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_EXCLUSIVE;
      case LESS_THAN_OR_EQUAL:
        return Range.LOWER_UNBOUNDED + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_INCLUSIVE;
      case BETWEEN:
        return Range.LOWER_INCLUSIVE + RequestUtils.getLiteralString(operands.get(1)) + Range.DELIMITER
            + RequestUtils.getLiteralString(operands.get(2)) + Range.UPPER_INCLUSIVE;
      case RANGE:
        return operands.get(1).getLiteral().getStringValue();
      default:
        throw new IllegalStateException();
    }
  }
}
