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
package org.apache.pinot.core.query.pruner;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ColumnValueSegmentPrunerTest {
  private static final ColumnValueSegmentPruner PRUNER = new ColumnValueSegmentPruner();

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);
  }

  @Test
  public void testMinMaxValuePruning() {
    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getMinValue()).thenReturn(10);
    when(dataSourceMetadata.getMaxValue()).thenReturn(20);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Equality predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 20"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 30"));
    // Range predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column <= 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 20"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 30"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 30 AND 40"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 10 AND 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 20"));
    // Invalid range predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 30 AND 20"));
    // In Predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 5, 8)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21, 30)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (10)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5, 10, 15)"));
    //although the segment can be pruned, it will not be pruned as the size of values is greater than threshold
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"));
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"));
    // AND operator
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 AND column > 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 0 AND column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 AND column <= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 20 AND column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 20 AND column < 30"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 0 AND column BETWEEN 0 AND 10"));
    // OR operator
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column > 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 OR column <= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 30 OR column < 10"));
    assertTrue(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 0 AND 5 OR column BETWEEN 30 AND 35"));
  }

  /**
   * Verifies that when a single range predicate is evaluated against multiple segments in one prune() call, the
   * converted bounds are cached once and reused correctly: each segment is kept or pruned based purely on its own
   * min/max range, and reuse of the cached bounds never corrupts a later segment's decision.
   */
  @Test
  public void testRangePruningReusesCachedBoundsAcrossSegments() {
    // Query range is [20321, 20686). Build segments that fall entirely below, overlapping, and entirely above it.
    IndexSegment below = mockIndexSegment(DataType.INT, 20140, 20200);      // max < lower -> pruned
    IndexSegment lowerEdge = mockIndexSegment(DataType.INT, 20300, 20321);  // max == lower (inclusive) -> kept
    IndexSegment overlap = mockIndexSegment(DataType.INT, 20330, 20400);    // within range -> kept
    IndexSegment upperEdge = mockIndexSegment(DataType.INT, 20686, 20700);  // min == upper (exclusive) -> pruned
    IndexSegment above = mockIndexSegment(DataType.INT, 20800, 20900);      // min > upper -> pruned

    List<IndexSegment> segments = Arrays.asList(below, lowerEdge, overlap, upperEdge, above);
    String query = "SELECT COUNT(*) FROM testTable WHERE column >= 20321 AND column < 20686";

    List<IndexSegment> survivors = runPruner(segments, query);

    // Only the segments whose range intersects [20321, 20686) survive; order of evaluation must not matter.
    assertEquals(survivors, Arrays.asList(lowerEdge, overlap));

    // Re-running the same segments in a different order yields the same per-segment decisions.
    List<IndexSegment> reordered = Arrays.asList(above, overlap, below, lowerEdge, upperEdge);
    assertEquals(runPruner(reordered, query), Arrays.asList(overlap, lowerEdge));
  }

  /**
   * Same as the reuse test above but using BETWEEN, which produces a single RANGE predicate with both bounds
   * inclusive ([lower, upper]). This mirrors what the server-side filter optimizer emits when it merges
   * {@code col >= x AND col <= y} into one range, and exercises the inclusive-lower / inclusive-upper branches.
   * With inclusive bounds the segment whose min equals the upper bound is kept (unlike the half-open case).
   */
  @Test
  public void testBetweenRangePruningReusesCachedBoundsAcrossSegments() {
    // Query range is [20321, 20686] (both inclusive).
    IndexSegment below = mockIndexSegment(DataType.INT, 20140, 20200);      // max < lower -> pruned
    IndexSegment lowerEdge = mockIndexSegment(DataType.INT, 20300, 20321);  // max == lower (inclusive) -> kept
    IndexSegment overlap = mockIndexSegment(DataType.INT, 20330, 20400);    // within range -> kept
    IndexSegment upperEdge = mockIndexSegment(DataType.INT, 20686, 20700);  // min == upper (inclusive) -> kept
    IndexSegment above = mockIndexSegment(DataType.INT, 20800, 20900);      // min > upper -> pruned

    List<IndexSegment> segments = Arrays.asList(below, lowerEdge, overlap, upperEdge, above);
    String query = "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20321 AND 20686";

    // Inclusive on both ends, so lowerEdge and upperEdge are both kept.
    assertEquals(runPruner(segments, query), Arrays.asList(lowerEdge, overlap, upperEdge));
  }

  /**
   * Verifies that the cached range bounds are re-converted when the column data type differs across segments (schema
   * evolution). The bounds "3"/"8" are valid for both LONG and DOUBLE; if a LONG segment reused a DOUBLE-converted
   * bound (or vice versa) the min/max comparison would use the wrong type. Each segment's min/max is typed to match
   * its own column type so the comparisons are apples-to-apples.
   */
  @Test
  public void testRangePruningHandlesHeterogeneousDataTypeAcrossSegments() {
    IndexSegment doubleKept = mockIndexSegment(DataType.DOUBLE, 0.0, 5.0);    // (3,8) intersects [0,5] -> kept
    IndexSegment doublePruned = mockIndexSegment(DataType.DOUBLE, 8.0, 9.0);  // min == upper (exclusive) -> pruned
    IndexSegment longKept = mockIndexSegment(DataType.LONG, 3L, 4L);          // [3,4] within (3,8) -> kept
    IndexSegment longPruned = mockIndexSegment(DataType.LONG, 10L, 20L);      // above 8 -> pruned

    // Interleave the types so the cache flips DOUBLE -> LONG -> DOUBLE -> LONG and must re-convert each time.
    List<IndexSegment> segments = Arrays.asList(doubleKept, longPruned, doublePruned, longKept);
    String query = "SELECT COUNT(*) FROM testTable WHERE column > 3 AND column < 8";

    assertEquals(runPruner(segments, query), Arrays.asList(doubleKept, longKept));
  }

  /**
   * Verifies that a half-open range (one UNBOUNDED side) is cached and reused correctly across multiple segments.
   */
  @Test
  public void testRangePruningWithUnboundedSideAcrossSegments() {
    IndexSegment below = mockIndexSegment(DataType.INT, 0, 50);        // max < 100 -> pruned
    IndexSegment edge = mockIndexSegment(DataType.INT, 90, 100);       // max == 100 (inclusive) -> kept
    IndexSegment above = mockIndexSegment(DataType.INT, 200, 300);     // within [100, +inf) -> kept

    // Only a lower bound; upper bound is UNBOUNDED.
    String query = "SELECT COUNT(*) FROM testTable WHERE column >= 100";

    assertEquals(runPruner(Arrays.asList(below, edge, above), query), Arrays.asList(edge, above));
  }

  @Test
  public void testPartitionPruning() {
    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getPartitionFunction()).thenReturn(
        PartitionFunctionFactory.getPartitionFunction("Modulo", 5, null));
    when(dataSourceMetadata.getPartitions()).thenReturn(Collections.singleton(2));
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Equality predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 2"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 7"));
    // AND operator
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 AND column = 2"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 AND column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 2 AND column > 0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column <= 10 AND column = 7"));
    // OR operator
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column = 2"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column < 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column = 10"));
  }

  @Test
  public void testIsApplicableTo() {
    // EQ, RANGE and IN (with small number of values) are applicable for min/max/partitionId based pruning.
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column = 1");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column IN (1, 2)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column BETWEEN 1 AND 2");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // NOT is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE NOT column = 1");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // Too many values for IN clause
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column IN (1, 2, 3, 4, 5, 6, 7)");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // Other predicate types are not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column LIKE 5");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // AND with one applicable child filter is applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column NOT IN (1, 2) AND column = 3");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // OR with one child filter that's not applicable is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR column NOT IN (1, 2)");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // Nested with AND/OR
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR (column NOT IN (1, 2) AND column BETWEEN 4 AND 5)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
  }

  private IndexSegment mockIndexSegment() {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("column"));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(20);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return indexSegment;
  }

  /**
   * Creates a segment whose single "column" reports the given data type and min/max range for pruning.
   */
  private IndexSegment mockIndexSegment(DataType dataType, Comparable minValue, Comparable maxValue) {
    IndexSegment indexSegment = mockIndexSegment();
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(dataType);
    when(dataSourceMetadata.getMinValue()).thenReturn(minValue);
    when(dataSourceMetadata.getMaxValue()).thenReturn(maxValue);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    return indexSegment;
  }

  private boolean runPruner(IndexSegment indexSegment, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(mock(Schema.class));
    return PRUNER.prune(Arrays.asList(indexSegment), queryContext).isEmpty();
  }

  /**
   * Prunes {@code query} across the given segments in a single {@code prune()} call (so the per-query ValueCache is
   * shared across all of them) and returns the surviving segments.
   */
  private List<IndexSegment> runPruner(List<IndexSegment> indexSegments, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(mock(Schema.class));
    return PRUNER.prune(new ArrayList<>(indexSegments), queryContext);
  }
}
