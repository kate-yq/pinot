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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.combine.NativeGroupByCombineRouter;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupByCombine;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end differential parity test for the native server-combine operator (design §26.7/§29.3)
 * over <b>all key types</b>. For each of INT/LONG/FLOAT/DOUBLE/STRING group-by keys it runs a
 * multi-segment GROUP BY with native combine disabled (Java {@code GroupByCombineOperator}) and
 * enabled ({@code NativeGroupByCombineOperator}) and asserts the broker responses are identical, in
 * the <b>no-approximation regime</b> where exact parity is well-defined:
 *
 * <ul>
 *   <li><b>No ORDER BY, {@code LIMIT} &ge; cardinality</b> — every group is returned, so the
 *       (otherwise arbitrary, §26.7-3) no-ORDER-BY selection is deterministic → exact set parity.</li>
 *   <li><b>ORDER BY under a total order</b> (group key, or an aggregation followed by the unique group
 *       key) with server-side trim forced on ({@code minServerGroupTrimSize}) — native computes an
 *       exact top-K and, because the order is total, Java's top-K is exact too (no intermediate resize
 *       below {@code trimThreshold}, in-segment trim gated off), so the ordered rows match exactly.
 *       This genuinely exercises native's top-K trim (1500 groups → 200) end-to-end.</li>
 * </ul>
 *
 * <p>The segment group-by stays Java (native segment flag off), so this isolates the native combine,
 * fed by Java segment partials. Multiple segment copies force a real cross-segment merge. Each native
 * run also asserts {@link NativeGroupByCombineRouter#shouldAccelerate} so a routing regression can't
 * make the differential vacuously pass by running Java on both sides.
 */
public class NativeGroupByCombineQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeGroupByCombineQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String LIB_PATH_PROP = "pinot.native.lib.path";

  // Enables the native engine via the broker-stamped internal query option (as production does).
  private static final Map<String, String> NATIVE_ON = Map.of(QueryOptionKey.NATIVE_ENGINE_ENABLED, "true");

  private static final String KEY_COL = "keyCol";
  private static final String KEY_COL2 = "keyCol2";
  private static final String LONG_VAL = "longVal";
  private static final String INT_VAL = "intVal";
  private static final String DOUBLE_VAL = "doubleVal";

  private static final int NUM_ROWS = 40_000;
  private static final int KEY_CARDINALITY = 1_500;
  private static final int KEY2_CARDINALITY = 40;
  private static final int NUM_SEGMENTS = 4;
  private static final int ORDER_BY_LIMIT = 25;

  private static final String MULTICOL_PROPERTY = "pinot.native.groupby.combine.multicol";

  /** {@code SELECT keyCol, SUM(longVal), MIN(intVal), MAX(doubleVal), COUNT(*) FROM testTable }. */
  private static final String SELECT =
      "SELECT " + KEY_COL + ", SUM(" + LONG_VAL + "), MIN(" + INT_VAL + "), MAX(" + DOUBLE_VAL + "), COUNT(*) "
          + "FROM " + RAW_TABLE_NAME + " ";
  // Force server-side trim so ORDER BY actually exercises native top-K (trimSize = max(LIMIT*5, 200)).
  private static final Map<String, String> TRIM_OPTION = Map.of(QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE, "200");

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
  public void checkNative() {
    if (!PinotNativeGroupByCombine.isAvailable()) {
      throw new SkipException("pinot-native library not loadable; build with './mvnw -pl pinot-native package'");
    }
  }

  @AfterMethod
  public void cleanup()
      throws Exception {
    System.clearProperty(MULTICOL_PROPERTY);
    if (_indexSegment != null) {
      _indexSegment.destroy();
      _indexSegment = null;
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @DataProvider(name = "keyTypes")
  public Object[][] keyTypes() {
    return new Object[][]{
        {DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}, {DataType.STRING}
    };
  }

  @Test(dataProvider = "keyTypes")
  public void nativeCombineMatchesJava(DataType keyType)
      throws Exception {
    buildSegments(keyType);
    String tag = keyType.toString();

    // No ORDER BY with LIMIT >= cardinality: every group is returned → deterministic, exact set parity.
    assertParity(SELECT + "GROUP BY " + KEY_COL + " LIMIT " + (KEY_CARDINALITY + 500), null, false,
        tag + " no-order-by");

    // ORDER BY under a total order + forced server trim: exact ordered parity that exercises top-K.
    assertParity(SELECT + "GROUP BY " + KEY_COL + " ORDER BY " + KEY_COL + " ASC LIMIT " + ORDER_BY_LIMIT,
        TRIM_OPTION, true, tag + " order-by-key-asc");
    assertParity(SELECT + "GROUP BY " + KEY_COL + " ORDER BY " + KEY_COL + " DESC LIMIT " + ORDER_BY_LIMIT,
        TRIM_OPTION, true, tag + " order-by-key-desc");
    // ORDER BY an aggregation column, made a total order via the unique group key (native breaks agg
    // ties by key asc; the explicit secondary key makes Java's trim deterministic too).
    assertParity(
        SELECT + "GROUP BY " + KEY_COL + " ORDER BY COUNT(*) DESC, " + KEY_COL + " ASC LIMIT " + ORDER_BY_LIMIT,
        TRIM_OPTION, true, tag + " order-by-count-desc");
    assertParity(
        SELECT + "GROUP BY " + KEY_COL + " ORDER BY SUM(" + LONG_VAL + ") ASC, " + KEY_COL + " ASC LIMIT "
            + ORDER_BY_LIMIT, TRIM_OPTION, true, tag + " order-by-sum-asc");
  }

  @DataProvider(name = "keyTypesAndStrategies")
  public Object[][] keyTypesAndStrategies() {
    DataType[] keyTypes = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.STRING};
    String[] strategies = {"columnwise", "packed"};
    Object[][] out = new Object[keyTypes.length * strategies.length][];
    int i = 0;
    for (DataType kt : keyTypes) {
      for (String st : strategies) {
        out[i++] = new Object[]{kt, st};
      }
    }
    return out;
  }

  /**
   * Multi-column GROUP BY (keyCol, keyCol2) — Java combine vs native combine under <b>both</b> merge
   * strategies (column-wise / packed). keyCol is the parameterized type; keyCol2 is a dict-encoded INT.
   * The segment group-by stays Java so this isolates the native multi-column combine.
   */
  @Test(dataProvider = "keyTypesAndStrategies")
  public void nativeMultiColumnCombineMatchesJava(DataType keyType, String strategy)
      throws Exception {
    buildSegments(keyType);
    String tag = keyType + "/" + strategy;
    System.setProperty(MULTICOL_PROPERTY, strategy);
    try {
      String twoKeySelect = "SELECT " + KEY_COL + ", " + KEY_COL2 + ", SUM(" + LONG_VAL + "), MIN(" + INT_VAL
          + "), MAX(" + DOUBLE_VAL + "), COUNT(*) FROM " + RAW_TABLE_NAME + " ";
      // No ORDER BY, LIMIT >= the full distinct (keyCol, keyCol2) cross-product (bounded by NUM_ROWS) →
      // every group returned → deterministic exact set parity (§26.7-3).
      assertParity(twoKeySelect + "GROUP BY " + KEY_COL + ", " + KEY_COL2 + " LIMIT 100000",
          null, false, tag + " multi no-order-by");
      // ORDER BY both key columns (total order) + forced trim → exact ordered top-K parity.
      assertParity(twoKeySelect + "GROUP BY " + KEY_COL + ", " + KEY_COL2 + " ORDER BY " + KEY_COL + " ASC, "
          + KEY_COL2 + " ASC LIMIT " + ORDER_BY_LIMIT, TRIM_OPTION, true, tag + " multi order-by-keys");
      // ORDER BY an aggregation, tie-broken by both keys (total order).
      assertParity(twoKeySelect + "GROUP BY " + KEY_COL + ", " + KEY_COL2 + " ORDER BY COUNT(*) DESC, " + KEY_COL
          + " ASC, " + KEY_COL2 + " ASC LIMIT " + ORDER_BY_LIMIT, TRIM_OPTION, true, tag + " multi order-by-count");
    } finally {
      System.clearProperty(MULTICOL_PROPERTY);
    }
  }

  /**
   * End-to-end: <b>both</b> the segment group-by AND the combine run native (both flags on), over a
   * multi-column INT+INT key (dict-encoded fixed-width → the segment packs dict-ids, the combine packs
   * raw values), vs the all-Java path. Proves the two native tiers compose.
   */
  @Test
  public void nativeMultiColumnEndToEndMatchesJava()
      throws Exception {
    buildSegments(DataType.INT);
    String twoKeySelect = "SELECT " + KEY_COL + ", " + KEY_COL2 + ", SUM(" + LONG_VAL + "), COUNT(*) FROM "
        + RAW_TABLE_NAME + " ";
    // LIMIT >= the full distinct cross-product (bounded by NUM_ROWS) → every group returned, deterministic.
    String query = twoKeySelect + "GROUP BY " + KEY_COL + ", " + KEY_COL2 + " LIMIT 100000";

    ResultTable javaResult = getBrokerResponse(query).getResultTable();
    assertFalse(javaResult.getRows().isEmpty(), "expected a non-empty Java result");

    ResultTable nativeResult = getBrokerResponse(query, NATIVE_ON).getResultTable();
    assertEquals(nativeResult.getDataSchema().toString(), javaResult.getDataSchema().toString(),
        "end-to-end: result schema differs");
    assertEquals(rowSet(nativeResult), rowSet(javaResult), "end-to-end native (segment+combine) rows differ");
  }

  /**
   * Run {@code query} with native combine off (Java) and on, and assert the broker responses match —
   * as an ordered row list when {@code ordered}, else as an unordered row set. Also asserts the router
   * accelerates the native run (so the comparison is not vacuously Java-vs-Java).
   */
  private void assertParity(String query, @Nullable Map<String, String> options, boolean ordered, String tag) {
    ResultTable javaResult = getBrokerResponse(query, options).getResultTable();
    assertFalse(javaResult.getRows().isEmpty(), tag + ": expected a non-empty Java result");

    assertTrue(NativeGroupByCombineRouter.shouldAccelerate(nativeQueryContext(query)),
        tag + ": router did not accelerate — native combine path was not exercised");
    ResultTable nativeResult = getBrokerResponse(query, withNative(options)).getResultTable();

    assertEquals(nativeResult.getDataSchema().toString(), javaResult.getDataSchema().toString(),
        tag + ": result schema differs");
    if (ordered) {
      assertEquals(rowList(nativeResult), rowList(javaResult), tag + ": ordered rows differ from Java");
    } else {
      assertEquals(rowSet(nativeResult), rowSet(javaResult), tag + ": rows differ from Java");
    }
  }

  /** The router accepts the supported shapes and rejects out-of-scope ones (design §26.7/§29.2). */
  @Test
  public void routerGating() {
    // Accepted: single key, SUM/MIN/MAX/COUNT, no HAVING, ORDER BY over group key / aggregation.
    assertTrue(accelerates(SELECT + "GROUP BY " + KEY_COL + " LIMIT 10"), "no-order-by should accelerate");
    assertTrue(accelerates(SELECT + "GROUP BY " + KEY_COL + " ORDER BY COUNT(*) DESC LIMIT 10"),
        "order-by-agg should accelerate");
    assertTrue(accelerates(SELECT + "GROUP BY " + KEY_COL + " ORDER BY " + KEY_COL + " ASC LIMIT 10"),
        "order-by-key should accelerate");

    assertTrue(accelerates("SELECT " + KEY_COL + ", " + KEY_COL2 + ", COUNT(*) FROM " + RAW_TABLE_NAME
        + " GROUP BY " + KEY_COL + ", " + KEY_COL2 + " LIMIT 10"), "multi-key should accelerate");

    // Rejected: HAVING, post-aggregation order key, unsupported aggregation.
    assertFalse(accelerates(
            SELECT + "GROUP BY " + KEY_COL + " HAVING COUNT(*) > 5 LIMIT 10"), "HAVING should not accelerate");
    assertFalse(accelerates(
            SELECT + "GROUP BY " + KEY_COL + " ORDER BY SUM(" + LONG_VAL + ") + 1 DESC LIMIT 10"),
        "post-aggregation order key should not accelerate");
    assertFalse(accelerates("SELECT " + KEY_COL + ", AVG(" + LONG_VAL + ") FROM " + RAW_TABLE_NAME
        + " GROUP BY " + KEY_COL + " LIMIT 10"), "unsupported aggregation should not accelerate");

    // The gate itself: with the native engine disabled, even a fully-eligible query must not accelerate.
    assertFalse(NativeGroupByCombineRouter.shouldAccelerate(
            QueryContextConverterUtils.getQueryContext(SELECT + "GROUP BY " + KEY_COL + " LIMIT 10")),
        "native engine disabled should not accelerate");
  }

  private static boolean accelerates(String query) {
    return NativeGroupByCombineRouter.shouldAccelerate(nativeQueryContext(query));
  }

  /** A QueryContext with the native engine enabled, mirroring the broker-stamped option at runtime. */
  private static QueryContext nativeQueryContext(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setNativeEngineEnabled(true);
    return queryContext;
  }

  /** {@code options} plus the native-engine gate, for the native leg of a parity comparison. */
  private static Map<String, String> withNative(@Nullable Map<String, String> options) {
    Map<String, String> merged = new HashMap<>();
    if (options != null) {
      merged.putAll(options);
    }
    merged.put(QueryOptionKey.NATIVE_ENGINE_ENABLED, "true");
    return merged;
  }

  private void buildSegments(DataType keyType)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    Random rng = new Random(20260620L);
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(KEY_COL, makeKey(keyType, rng.nextInt(KEY_CARDINALITY)));
      row.putValue(KEY_COL2, rng.nextInt(KEY2_CARDINALITY));
      row.putValue(LONG_VAL, (long) (rng.nextInt(2_000_000) - 1_000_000));
      row.putValue(INT_VAL, rng.nextInt(1_000_000) - 500_000);
      row.putValue(DOUBLE_VAL, rng.nextDouble() * 1000.0 - 500.0);
      rows.add(row);
    }
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(KEY_COL, keyType)
        .addSingleValueDimension(KEY_COL2, DataType.INT)
        .addMetric(LONG_VAL, DataType.LONG)
        .addMetric(INT_VAL, DataType.INT)
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
    IndexSegment[] copies = new IndexSegment[NUM_SEGMENTS];
    Arrays.fill(copies, immutableSegment);
    _indexSegments = Arrays.asList(copies);
  }

  private static Object makeKey(DataType keyType, int raw) {
    switch (keyType) {
      case INT:
        return raw;
      case LONG:
        return (long) raw;
      case FLOAT:
        return raw * 0.25f;
      case DOUBLE:
        return raw * 0.25;
      default:
        return "k-" + raw;
    }
  }

  private static Set<List<Object>> rowSet(ResultTable resultTable) {
    Set<List<Object>> set = new HashSet<>();
    for (Object[] row : resultTable.getRows()) {
      set.add(Arrays.asList(row));
    }
    return set;
  }

  private static List<List<Object>> rowList(ResultTable resultTable) {
    List<List<Object>> list = new ArrayList<>(resultTable.getRows().size());
    for (Object[] row : resultTable.getRows()) {
      list.add(Arrays.asList(row));
    }
    return list;
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
    return Files.exists(candidate) ? candidate.toString() : null;
  }
}
