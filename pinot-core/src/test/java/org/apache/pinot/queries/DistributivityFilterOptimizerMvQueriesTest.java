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
 * Multi-value execution-level equivalence tests for the
 * {@link org.apache.pinot.core.query.optimizer.filter.DistributivityFilterOptimizer}.
 *
 * <p>Distributivity is a pure boolean restructuring: the optimizer treats each leaf predicate as an opaque expression
 * and never inspects the column type or nullability, so for this rewrite single-value and multi-value columns make no
 * difference. This file is therefore intentionally kept minimal &mdash; a lightweight integration guard, not a
 * duplicate of the single-value suite ({@link DistributivityFilterOptimizerQueriesTest}). It covers just two shapes
 * on a multi-value column: one vanilla factoring scenario scaled to {@value #NUM_BRANCHES} branches, and one shape
 * that pushes real UNKNOWN rows through a nullable multi-value column under Kleene three-valued logic.
 *
 * <p>It builds a self-contained segment with a multi-value inverted-indexed INT column ({@code mvInt}), a single-value
 * INT column ({@code svInt}) used as the common conjunct, a nullable multi-value INT column ({@code nullableMv}, with
 * some rows null), and an INT metric ({@code met}). For each shape it asserts that executing the query as written (raw
 * filter tree, optimizer NOT run) yields the same result as executing it after the
 * {@link org.apache.pinot.core.query.optimizer.QueryOptimizer} chain rewrites it.
 */
public class DistributivityFilterOptimizerMvQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "DistributivityFilterOptimizerMvQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String SV_INT = "svInt";
  private static final String MV_INT = "mvInt";
  private static final String NULLABLE_MV = "nullableMv";
  private static final String MET = "met";

  private static final int NUM_BRANCHES = 100;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(SV_INT, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_INT, FieldSpec.DataType.INT)
      .addMultiValueDimension(NULLABLE_MV, FieldSpec.DataType.INT)
      .addMetric(MET, FieldSpec.DataType.INT)
      .build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .setInvertedIndexColumns(Arrays.asList(SV_INT, MV_INT))
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
    // svInt, mvInt, nullableMv, met
    rows.add(row(5, new Integer[]{0, 1}, new Integer[]{5}, 10));
    rows.add(row(5, new Integer[]{1, 2}, null, 20));       // null in the nullable MV column
    rows.add(row(5, new Integer[]{2, 3}, new Integer[]{5, 7}, 30));
    rows.add(row(5, new Integer[]{3}, new Integer[]{7}, 40));
    rows.add(row(7, new Integer[]{0}, new Integer[]{5}, 50));
    rows.add(row(7, new Integer[]{2, 3}, null, 60));       // null in the nullable MV column

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

  private static GenericRow row(int svInt, Integer[] mvInt, Integer[] nullableMv, int met) {
    GenericRow row = new GenericRow();
    row.putValue(SV_INT, svInt);
    row.putValue(MV_INT, mvInt);
    row.putValue(NULLABLE_MV, nullableMv);
    row.putValue(MET, met);
    return row;
  }

  @DataProvider(name = "mvShapes")
  public Object[][] mvShapes() {
    return new Object[][]{
        // Vanilla factoring on a multi-value residual, scaled to NUM_BRANCHES branches.
        {"mv_factorable_or"},
        // Nullable multi-value column with 3VL null handling enabled.
        {"mv_null_common"}
    };
  }

  @Test(dataProvider = "mvShapes")
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
    switch (shape) {
      case "mv_factorable_or":
        // Common conjunct on the single-value column, wide residual on the multi-value column:
        //   (svInt = 5 AND mvInt = 0) OR ... OR (svInt = 5 AND mvInt = 99)  ->  svInt = 5 AND mvInt IN (0..99)
        return groupedAgg(orOfAnds(NUM_BRANCHES), false);
      case "mv_null_common":
        // Common conjunct on a nullable multi-value column that is UNKNOWN (null) for some rows (Kleene 3VL):
        //   (nullableMv = 5 AND svInt = 5) OR (nullableMv = 5 AND svInt = 7)  ->  nullableMv = 5 AND svInt IN (5, 7)
        return groupedAgg("(nullableMv = 5 AND svInt = 5) OR (nullableMv = 5 AND svInt = 7)", true);
      default:
        throw new IllegalArgumentException("Unknown shape: " + shape);
    }
  }

  private String groupedAgg(String whereClause, boolean enableNullHandling) {
    String prefix = enableNullHandling ? "SET enableNullHandling=true; " : "";
    return prefix + "SELECT svInt, COUNT(*), SUM(met) FROM testTable WHERE " + whereClause
        + " GROUP BY svInt ORDER BY svInt LIMIT 1000";
  }

  private String orOfAnds(int numBranches) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("(svInt = 5 AND mvInt = ").append(i).append(')');
    }
    return sb.toString();
  }
}
