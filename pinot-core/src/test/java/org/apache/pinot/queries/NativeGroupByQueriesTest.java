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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end differential test for the native (Rust+JNI) segment GROUP BY path (design doc §19
 * step 1b). Runs a GROUP BY query twice over the same in-memory segment(s) — once with the native
 * engine disabled (Java {@code DefaultGroupByExecutor}) and once enabled
 * ({@code NativeGroupByExecutor}) — and asserts the full broker responses are identical.
 *
 * <p>The native path must match the Java path bit-for-bit: SUM accumulates in f64 (Pinot's group-by
 * SUM semantics), MIN/MAX stay typed (lossless when viewed as double), COUNT is exact. The query
 * exercises every native agg-kind mapping over INT/LONG/FLOAT/DOUBLE value columns plus COUNT.
 *
 * <p>The native library is loaded from the sibling module's Cargo output; the suite is skipped if it
 * is not present ({@code ./mvnw -pl pinot-native package} produces it).
 */
public class NativeGroupByQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeGroupByQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String LIB_PATH_PROP = "pinot.native.lib.path";

  // Enables the native engine the way production does — via the broker-stamped internal query option
  // (resolved from table/cluster config). No system property / global state involved.
  private static final Map<String, String> NATIVE_ON = Map.of(QueryOptionKey.NATIVE_ENGINE_ENABLED, "true");

  private static final String KEY_COL = "keyCol";
  private static final String KEY_COL2 = "keyCol2";
  private static final String STR_KEY = "strKey";
  private static final String INT_VAL = "intVal";
  private static final String LONG_VAL = "longVal";
  private static final String FLOAT_VAL = "floatVal";
  private static final String DOUBLE_VAL = "doubleVal";

  private static final int NUM_ROWS = 20_000;
  private static final int KEY_CARDINALITY = 50;
  private static final int KEY2_CARDINALITY = 40;
  private static final int STR_CARDINALITY = 60;

  private static final String GROUP_BY_QUERY =
      "SELECT " + KEY_COL + ", "
          + "SUM(" + INT_VAL + "), SUM(" + LONG_VAL + "), SUM(" + FLOAT_VAL + "), SUM(" + DOUBLE_VAL + "), "
          + "MIN(" + INT_VAL + "), MIN(" + LONG_VAL + "), MIN(" + FLOAT_VAL + "), MIN(" + DOUBLE_VAL + "), "
          + "MAX(" + INT_VAL + "), MAX(" + LONG_VAL + "), MAX(" + FLOAT_VAL + "), MAX(" + DOUBLE_VAL + "), "
          + "COUNT(*) "
          + "FROM " + RAW_TABLE_NAME + " GROUP BY " + KEY_COL + " LIMIT 1000";

  // Multi-column GROUP BY over two dict-encoded fixed-width keys — exercises the native packed-i64
  // key path (design §17.9). 50 x 40 cardinalities → 6 + 6 = 12 bits, well within the 64-bit bound.
  private static final String MULTI_GROUP_BY_QUERY =
      "SELECT " + KEY_COL + ", " + KEY_COL2 + ", "
          + "SUM(" + LONG_VAL + "), MIN(" + INT_VAL + "), MAX(" + DOUBLE_VAL + "), COUNT(*) "
          + "FROM " + RAW_TABLE_NAME + " GROUP BY " + KEY_COL + ", " + KEY_COL2 + " LIMIT 5000";

  // Single dict-encoded STRING key — exercises native STRING-at-segment (dict-id group, decode via
  // Dictionary.getInternal at the drain boundary; design §25.5). Native segment + native combine.
  private static final String STRING_GROUP_BY_QUERY =
      "SELECT " + STR_KEY + ", "
          + "SUM(" + INT_VAL + "), MIN(" + LONG_VAL + "), MAX(" + DOUBLE_VAL + "), COUNT(*) "
          + "FROM " + RAW_TABLE_NAME + " GROUP BY " + STR_KEY + " LIMIT 1000";

  // Multi-column GROUP BY mixing a STRING key with a fixed-width key — the packed path packs both
  // dict-ids (STRING dict-id is fixed-width too). 60 x 50 → 6 + 6 = 12 bits, within 64.
  private static final String STRING_MULTI_GROUP_BY_QUERY =
      "SELECT " + STR_KEY + ", " + KEY_COL + ", "
          + "SUM(" + LONG_VAL + "), MIN(" + INT_VAL + "), MAX(" + DOUBLE_VAL + "), COUNT(*) "
          + "FROM " + RAW_TABLE_NAME + " GROUP BY " + STR_KEY + ", " + KEY_COL + " LIMIT 5000";

  // Resolve the dev-built native lib before PinotNativeGroupBy is first referenced.
  static {
    String resolved = resolveDevLibPath();
    if (resolved != null && System.getProperty(LIB_PATH_PROP) == null) {
      System.setProperty(LIB_PATH_PROP, resolved);
    }
  }

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
    if (!PinotNativeGroupBy.isAvailable()) {
      throw new SkipException("pinot-native library not loadable. Build it with "
          + "'./mvnw -pl pinot-native package' first. Searched at " + System.getProperty(LIB_PATH_PROP));
    }

    FileUtils.deleteDirectory(INDEX_DIR);
    Random rng = new Random(20260619L);
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(KEY_COL, rng.nextInt(KEY_CARDINALITY));
      row.putValue(KEY_COL2, rng.nextInt(KEY2_CARDINALITY));
      row.putValue(STR_KEY, "str_" + rng.nextInt(STR_CARDINALITY));
      row.putValue(INT_VAL, rng.nextInt(1_000_000) - 500_000);
      row.putValue(LONG_VAL, (long) (rng.nextInt(2_000_000) - 1_000_000));
      row.putValue(FLOAT_VAL, rng.nextFloat() * 1000.0f - 500.0f);
      row.putValue(DOUBLE_VAL, rng.nextDouble() * 1000.0 - 500.0);
      rows.add(row);
    }

    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(KEY_COL, DataType.INT)
        .addSingleValueDimension(KEY_COL2, DataType.INT)
        .addSingleValueDimension(STR_KEY, DataType.STRING)
        .addMetric(INT_VAL, DataType.INT)
        .addMetric(LONG_VAL, DataType.LONG)
        .addMetric(FLOAT_VAL, DataType.FLOAT)
        .addMetric(DOUBLE_VAL, DataType.DOUBLE)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    // Two segments to exercise the cross-segment combine path feeding off the native segment output.
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void nativeGroupByMatchesJava() {
    ResultTable javaResult = getBrokerResponse(GROUP_BY_QUERY).getResultTable();
    ResultTable nativeResult = getBrokerResponse(GROUP_BY_QUERY, NATIVE_ON).getResultTable();

    assertEquals(nativeResult.getDataSchema().toString(), javaResult.getDataSchema().toString(),
        "result schema differs between native and Java paths");
    assertFalse(javaResult.getRows().isEmpty(), "expected a non-empty Java result to compare against");
    assertEquals(rowSet(nativeResult), rowSet(javaResult),
        "native GROUP BY rows differ from the Java reference");
  }

  @Test
  public void nativeMultiColumnGroupByMatchesJava() {
    ResultTable javaResult = getBrokerResponse(MULTI_GROUP_BY_QUERY).getResultTable();
    ResultTable nativeResult = getBrokerResponse(MULTI_GROUP_BY_QUERY, NATIVE_ON).getResultTable();

    assertEquals(nativeResult.getDataSchema().toString(), javaResult.getDataSchema().toString(),
        "multi-column result schema differs between native and Java paths");
    assertFalse(javaResult.getRows().isEmpty(), "expected a non-empty Java result to compare against");
    // Two-column dict GROUP BY should enumerate close to the full 50 x 40 cross-product of keys.
    assertTrue(javaResult.getRows().size() > 1000,
        "expected many multi-column groups, got " + javaResult.getRows().size());
    assertEquals(rowSet(nativeResult), rowSet(javaResult),
        "native multi-column GROUP BY rows differ from the Java reference");
  }

  /**
   * Single dict-encoded STRING key must match Java through the native segment path (STRING-at-segment,
   * design §25.5): the segment groups on the STRING column's fixed-width dict-ids and decodes to raw
   * strings at the drain boundary, then the native combine's arena consumes them.
   */
  @Test
  public void nativeStringGroupByMatchesJava() {
    ResultTable javaResult = getBrokerResponse(STRING_GROUP_BY_QUERY).getResultTable();
    ResultTable nativeResult = getBrokerResponse(STRING_GROUP_BY_QUERY, NATIVE_ON).getResultTable();

    assertEquals(nativeResult.getDataSchema().toString(), javaResult.getDataSchema().toString(),
        "STRING-key result schema differs between native and Java paths");
    assertFalse(javaResult.getRows().isEmpty(), "expected a non-empty Java result to compare against");
    assertEquals(rowSet(nativeResult), rowSet(javaResult),
        "native STRING-key GROUP BY rows differ from the Java reference");
  }

  /**
   * Multi-column GROUP BY mixing a STRING key with a fixed-width key must match Java: the segment packs
   * both columns' dict-ids (the STRING dict-id is fixed-width too) and decodes per column at the drain.
   */
  @Test
  public void nativeStringMultiColumnGroupByMatchesJava() {
    ResultTable javaResult = getBrokerResponse(STRING_MULTI_GROUP_BY_QUERY).getResultTable();
    ResultTable nativeResult = getBrokerResponse(STRING_MULTI_GROUP_BY_QUERY, NATIVE_ON).getResultTable();

    assertEquals(nativeResult.getDataSchema().toString(), javaResult.getDataSchema().toString(),
        "STRING multi-column result schema differs between native and Java paths");
    assertFalse(javaResult.getRows().isEmpty(), "expected a non-empty Java result to compare against");
    assertEquals(rowSet(nativeResult), rowSet(javaResult),
        "native STRING multi-column GROUP BY rows differ from the Java reference");
  }

  // Order-insensitive comparison: group-by result row order is not guaranteed without ORDER BY.
  private static Set<List<Object>> rowSet(ResultTable resultTable) {
    Set<List<Object>> set = new HashSet<>();
    for (Object[] row : resultTable.getRows()) {
      set.add(Arrays.asList(row));
    }
    return set;
  }

  @Nullable
  private static String resolveDevLibPath() {
    String os = System.getProperty("os.name", "").toLowerCase();
    String libFile;
    if (os.contains("mac") || os.contains("darwin")) {
      libFile = "libpinot_native.dylib";
    } else if (os.contains("windows")) {
      libFile = "pinot_native.dll";
    } else {
      libFile = "libpinot_native.so";
    }
    Path candidate = Paths.get("..", "pinot-native", "native", "target", "release", libFile).toAbsolutePath();
    if (Files.exists(candidate)) {
      return candidate.toString();
    }
    return null;
  }
}
