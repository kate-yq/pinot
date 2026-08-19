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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Single-value execution-level equivalence suite for the
 * {@link org.apache.pinot.core.query.optimizer.filter.DistributivityFilterOptimizer}.
 *
 * <p>For each filter shape in the {@link #executionShapes()} provider it executes the query as written (via
 * {@link #getBrokerResponse}, which does NOT run the filter optimizer) and after optimization (via
 * {@link #getBrokerResponseForOptimizedQuery}, which runs the
 * {@link org.apache.pinot.core.query.optimizer.QueryOptimizer} chain), and asserts the two produce identical results on
 * a real segment. This validates that the rewrite is result-preserving across the permutations the optimizer impacts,
 * including the motivating pathological "OR of ANDs" shape scaled out to {@value #NUM_BRANCHES} branches.
 *
 * <p>It builds a self-contained segment (rather than reusing {@link BaseSingleValueQueriesTest}) so it can (a) use
 * deterministic {@code accountId} / {@code campaignId} values instead of data-discovered ones and (b) include a
 * nullable single-value column to exercise NULL handling (Kleene three-valued logic) in the same file. The schema has
 * inverted-indexed INT dimensions {@code accountId} (the common conjunct) and {@code campaignId} (100+ distinct values,
 * the residual), an auxiliary INT dimension {@code other}, a nullable INT dimension {@code nullableInt} (some rows
 * null), a {@code ts} LONG for the time-range predicate, and an INT metric {@code met}.
 */
public class DistributivityFilterOptimizerQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "DistributivityFilterOptimizerQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String ACCOUNT_ID = "accountId";
  private static final String CAMPAIGN_ID = "campaignId";
  private static final String OTHER = "other";
  private static final String NULLABLE_INT = "nullableInt";
  private static final String TS = "ts";
  private static final String MET = "met";

  private static final int NUM_BRANCHES = 100;
  private static final int ACCOUNT_A = 5;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(ACCOUNT_ID, FieldSpec.DataType.INT)
      .addSingleValueDimension(CAMPAIGN_ID, FieldSpec.DataType.INT)
      .addSingleValueDimension(OTHER, FieldSpec.DataType.INT)
      .addSingleValueDimension(NULLABLE_INT, FieldSpec.DataType.INT)
      .addSingleValueDimension(TS, FieldSpec.DataType.LONG)
      .addMetric(MET, FieldSpec.DataType.INT)
      .build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .setInvertedIndexColumns(Arrays.asList(ACCOUNT_ID, CAMPAIGN_ID))
      .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> rows = new ArrayList<>();
    // accountId, campaignId, other, nullableInt, ts, met
    rows.add(row(5, 0, 10, 5, 100000L, 10));
    rows.add(row(5, 1, 11, null, 100001L, 20));    // null in the nullable column
    rows.add(row(5, 2, 12, 7, 100002L, 30));
    rows.add(row(5, 3, -1, 5, 100003L, 40));       // negative "other" so "other >= 0" is selective
    rows.add(row(5, 4, 14, null, 100004L, 50));    // null in the nullable column
    rows.add(row(5, 5, 15, 5, 100008L, 90));
    rows.add(row(7, 0, 20, 5, 100005L, 60));
    rows.add(row(7, 1, 21, 7, 100006L, 70));
    rows.add(row(7, 2, 22, null, 100007L, 80));    // null in the nullable column

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setDefaultNullHandlingEnabled(true);
    config.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private static GenericRow row(int accountId, int campaignId, int other, Integer nullableInt, long ts, int met) {
    GenericRow row = new GenericRow();
    row.putValue(ACCOUNT_ID, accountId);
    row.putValue(CAMPAIGN_ID, campaignId);
    row.putValue(OTHER, other);
    row.putValue(NULLABLE_INT, nullableInt);
    row.putValue(TS, ts);
    row.putValue(MET, met);
    return row;
  }

  @DataProvider(name = "executionShapes")
  public Object[][] executionShapes() {
    return new Object[][]{
        {"factorable_or"},
        {"original_shape_superset_in_and_gt0"},
        {"absorption"},
        {"partial_noop"},
        {"nested_in_and"},
        {"count_no_group_by"},
        {"not_wrapping"},
        {"residual_stays_or"},
        // Nullable single-value column with 3VL null handling enabled.
        {"null_common"},
        {"null_residual"}
    };
  }

  @Test(dataProvider = "executionShapes")
  public void testExecutionEquivalence(String shape) {
    String query = buildQuery(shape);
    ResultTable original = getBrokerResponse(query).getResultTable();
    ResultTable rewritten = getBrokerResponseForOptimizedQuery(query, TABLE_CONFIG, SCHEMA).getResultTable();

    assertEquals(rewritten.getDataSchema(), original.getDataSchema(), "Schema mismatch for shape: " + shape);
    List<Object[]> originalRows = original.getRows();
    List<Object[]> rewrittenRows = rewritten.getRows();
    assertTrue(originalRows.size() > 0, "Expected shape '" + shape + "' to match some rows (non-vacuous comparison)");
    assertEquals(rewrittenRows.size(), originalRows.size(), "Row count mismatch for shape: " + shape);
    for (int i = 0; i < originalRows.size(); i++) {
      assertEquals(rewrittenRows.get(i), originalRows.get(i), "Row " + i + " mismatch for shape: " + shape);
    }
  }

  private String buildQuery(String shape) {
    String orOfAnds = orOfAnds(NUM_BRANCHES);
    switch (shape) {
      case "factorable_or":
        // (accountId = A AND campaignId = c1) OR ... OR (accountId = A AND campaignId = cN)
        return groupedAgg(orOfAnds, false);
      case "original_shape_superset_in_and_gt0":
        // The motivating pathological shape: time range + superset IN + redundant "> 0" + the wide OR-of-ANDs.
        return groupedAgg("ts >= 100000 AND ts <= 100008 AND campaignId IN (" + inList()
            + ") AND accountId > 0 AND campaignId > 0 AND (" + orOfAnds + ")", false);
      case "absorption":
        // (accountId = A AND campaignId = c1) OR accountId = A  ==  accountId = A
        return groupedAgg("(accountId = " + ACCOUNT_A + " AND campaignId = 0) OR accountId = " + ACCOUNT_A, false);
      case "partial_noop":
        // A stray branch (other >= 0) shares nothing, so no globally-common conjunct exists and the optimizer is a
        // no-op; execution must still be identical.
        return groupedAgg("(" + orOfAnds + ") OR other >= 0", false);
      case "nested_in_and":
        // OR-of-ANDs nested several levels down inside a top-level AND.
        return groupedAgg("other >= 0 AND (" + orOfAnds + ")", false);
      case "count_no_group_by":
        // Aggregation without GROUP BY (single-row result), exercising the non-group-by path.
        return "SELECT COUNT(*) FROM testTable WHERE " + orOfAnds;
      case "not_wrapping":
        // OR-of-ANDs wrapped in a NOT. Distributivity factors inside the NOT; the complement must be identical.
        return "SELECT COUNT(*) FROM testTable WHERE NOT (" + orOfAnds + ")";
      case "residual_stays_or":
        // Common conjunct (accountId = A) with residuals on different columns/kinds, so the residual stays an OR
        // (does not merge to IN): (accountId = A AND campaignId = 0) OR (accountId = A AND other > 0).
        return groupedAgg("(accountId = " + ACCOUNT_A + " AND campaignId = 0) OR (accountId = " + ACCOUNT_A
            + " AND other > 0)", false);
      case "null_common":
        // Common conjunct on a nullable column that is UNKNOWN for some rows (Kleene 3VL):
        //   (nullableInt = 5 AND accountId = 5) OR (nullableInt = 5 AND accountId = 7)
        //     ->  nullableInt = 5 AND accountId IN (5, 7)
        return groupedAgg("(nullableInt = 5 AND accountId = 5) OR (nullableInt = 5 AND accountId = 7)", true);
      case "null_residual":
        // Residual on a nullable column with 3VL null handling:
        //   (accountId = 5 AND nullableInt = 5) OR (accountId = 5 AND nullableInt = 7)
        //     ->  accountId = 5 AND nullableInt IN (5, 7)
        return groupedAgg("(accountId = 5 AND nullableInt = 5) OR (accountId = 5 AND nullableInt = 7)", true);
      default:
        throw new IllegalArgumentException("Unknown shape: " + shape);
    }
  }

  private String groupedAgg(String whereClause, boolean enableNullHandling) {
    String prefix = enableNullHandling ? "SET enableNullHandling=true; " : "";
    return prefix + "SELECT accountId, campaignId, SUM(met), COUNT(*) FROM testTable WHERE " + whereClause
        + " GROUP BY accountId, campaignId ORDER BY accountId, campaignId LIMIT 1000";
  }

  private String orOfAnds(int numBranches) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("(accountId = ").append(ACCOUNT_A).append(" AND campaignId = ").append(i).append(')');
    }
    return sb.toString();
  }

  private String inList() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < NUM_BRANCHES; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(i);
    }
    return sb.toString();
  }
}
