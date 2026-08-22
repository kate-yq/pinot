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
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
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


/**
 * Ad-hoc GROUP BY timing sweep (also a row-parity gate) over synthetic segments, comparing the pure
 * JVM path vs the native (segment + combine + native-serialize) path through the FULL server + broker
 * round trip ({@code getBrokerResponse}). Reliable because it runs the reactor classes directly (no
 * shaded-jar staleness); collects baseline/native row-count parity for every cell and asserts it at the
 * end (so the whole table always prints even if one cell diverges).
 *
 * <p><b>Data — a cardinality ladder, not one pool.</b> "Anchor" dimensions are drawn INDEPENDENTLY at
 * FIXED cardinalities ({@code c32, c100a, c100b, c100c, c256, c1000, str1000, long1000}); one scaling
 * dimension {@code dimHigh} is drawn from a range >> rows so its per-segment cardinality ~= rows/segment.
 * Because the anchor cardinalities never move, each multi-column shape pins ONE tier of the segment
 * ladder (T1 bit-packed-direct Σbits≤20 / T0 radix-direct ∏≤2^20 / T2 bit-packed-hash) at EVERY size —
 * scaling row count + dimHigh grows work and group counts without degenerating every shape into T2.
 * Plus 4 metrics (INT/FLOAT/LONG/DOUBLE, random). All queries set {@code numGroupsLimit = 20,000,000}
 * and {@code groupTrimThreshold ≈ 1e9} (disable Java intermediate trim → full-merge parity with native).
 *
 * <p><b>Query shapes</b> (each computes SUM/MIN/MAX over all 4 metrics = 12 aggregations, ORDER BY the
 * full group key + LIMIT): S1/S2/S3 single-column (STRING/LONG/INT); T1a/T1b → tier T1; T0a → tier T0;
 * T2a → tier T2. The sweep is run for each total-row size in {@link #DEFAULT_SIZES}.
 *
 * <pre>
 *   ./mvnw -pl pinot-native package -DskipTests    # build the native lib first
 *   ./mvnw -pl pinot-core test -Dtest=NativeGroupBySweepTest -o -Dsurefire.failIfNoSpecifiedTests=false
 *   # defaults: sizes {50K,500K,2M} x LIMITs {1K,2.5K,5K,10K,20K,50K,100K} x 7 shapes (~18 min)
 *   # options: -Dsweep.sizes=50000,500000,2000000  -Dsweep.segments=10
 *   #          -Dsweep.limits=1000,2500,5000,10000,20000,50000,100000
 *   #          -Dsweep.filter=T1   (only shapes whose label contains "T1")
 * </pre>
 */
public class NativeGroupBySweepTest extends BaseQueriesTest {
  private static final String LIB_PATH_PROP = "pinot.native.lib.path";
  private static final String SERIALIZE_PROPERTY = "pinot.native.groupby.combine.serialize";
  private static final String MULTICOL_PROPERTY = "pinot.native.groupby.combine.multicol";

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeGroupBySweepTest");
  private static final String RAW_TABLE_NAME = "testTable";

  // Fixed-cardinality "anchor" dimensions, drawn INDEPENDENTLY so the group count of a multi-column
  // shape is ~= the product of the cardinalities (capped by rows). Their cardinality is IDENTICAL at
  // every segment size, so each multi-column shape deterministically lands in ONE tier of the segment
  // ladder (T1 bit-packed-direct Σbits<=20 / T0 radix-direct ∏<=2^20 / T2 bit-packed-hash) regardless
  // of row count — the ladder never degenerates into "all T2".
  private static final String DIM_C32 = "dimC32";         // card 32   -> 5 bits
  private static final String DIM_C100A = "dimC100a";     // card 100  -> 7 bits
  private static final String DIM_C100B = "dimC100b";     // card 100  -> 7 bits
  private static final String DIM_C100C = "dimC100c";     // card 100  -> 7 bits
  private static final String DIM_C256 = "dimC256";       // card 256  -> 8 bits
  private static final String DIM_C1000 = "dimC1000";     // card 1000 -> 10 bits
  private static final String DIM_STR1000 = "dimStr1000"; // STRING card 1000 (single-column STRING path)
  private static final String DIM_LONG1000 = "dimLong1000"; // LONG card 1000 (single-column LONG path)
  // Scaling high-cardinality dimension: draw range >> rows, so its per-segment dictionary cardinality
  // ~= rows-per-segment and grows with the sweep. Feeds T2 (multi-column) and the single-column path.
  private static final String DIM_HIGH = "dimHigh";       // INT, card ~= rows-per-segment

  private static final int CARD_C32 = 32;
  private static final int CARD_C100 = 100;
  private static final int CARD_C256 = 256;
  private static final int CARD_C1000 = 1000;

  private static final String INT_MET = "intMet";
  private static final String FLOAT_MET = "floatMet";
  private static final String LONG_MET = "longMet";
  private static final String DOUBLE_MET = "doubleMet";

  // Row-count sweep (total rows). Each size is split across sweep.segments segments (default 10) and
  // rebuilt fresh. Anchor-dim cardinalities are FIXED (above); only row count and dimHigh's cardinality
  // scale with size, so T1/T0/T2 stay pinned per shape while per-row / boundary-1 drain cost is measured
  // across ~40x of scale. The default LIMIT range sweeps top-K (1000) up to full-group-set (100000) so
  // the LIMIT sensitivity (native's advantage is largest at small LIMIT, narrows as LIMIT -> group count)
  // is captured. A full default run is ~18 min. Override: -Dsweep.sizes=50000,500000,2000000
  // -Dsweep.segments=10  -Dsweep.limits=1000,2500,5000,10000,20000,50000,100000  -Dsweep.filter=T1
  private static final int[] DEFAULT_SIZES = {50_000, 500_000, 2_000_000};
  private static final int[] DEFAULT_LIMITS = {1_000, 2_500, 5_000, 10_000, 20_000, 50_000, 100_000};

  private static final Map<String, String> QUERY_OPTIONS = Map.of(
      QueryOptionKey.NUM_GROUPS_LIMIT, "20000000",
      QueryOptionKey.GROUP_TRIM_THRESHOLD, "1000000000");
  // Same options plus the native-engine gate (as the broker would stamp from table/cluster config).
  private static final Map<String, String> QUERY_OPTIONS_NATIVE = Map.of(
      QueryOptionKey.NUM_GROUPS_LIMIT, "20000000",
      QueryOptionKey.GROUP_TRIM_THRESHOLD, "1000000000",
      QueryOptionKey.NATIVE_ENGINE_ENABLED, "true");

  static {
    String resolved = resolveDevLibPath();
    if (resolved != null && System.getProperty(LIB_PATH_PROP) == null) {
      System.setProperty(LIB_PATH_PROP, resolved);
    }
  }

  private List<IndexSegment> _segments;
  private Schema _schema;
  private TableConfig _tableConfig;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segments.get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _segments;
  }

  @BeforeClass
  public void setUp() {
    // This is a manual performance benchmark (a full default run is ~18 min locally and ~60+ min on a
    // slow CI runner), NOT a correctness unit test — running it in the standard suite blows past the
    // surefire fork timeout. Skip by default; opt in with -Dsweep.run=true. Row parity is already
    // asserted by NativeGroupByQueriesTest / NativeGroupByCombineQueriesTest, which DO run in CI.
    if (!Boolean.getBoolean("sweep.run")) {
      throw new SkipException("perf sweep disabled; pass -Dsweep.run=true to run NativeGroupBySweepTest");
    }
    _schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(DIM_C32, DataType.INT)
        .addSingleValueDimension(DIM_C100A, DataType.INT)
        .addSingleValueDimension(DIM_C100B, DataType.INT)
        .addSingleValueDimension(DIM_C100C, DataType.INT)
        .addSingleValueDimension(DIM_C256, DataType.INT)
        .addSingleValueDimension(DIM_C1000, DataType.INT)
        .addSingleValueDimension(DIM_STR1000, DataType.STRING)
        .addSingleValueDimension(DIM_LONG1000, DataType.LONG)
        .addSingleValueDimension(DIM_HIGH, DataType.INT)
        .addMetric(INT_MET, DataType.INT)
        .addMetric(FLOAT_MET, DataType.FLOAT)
        .addMetric(LONG_MET, DataType.LONG)
        .addMetric(DOUBLE_MET, DataType.DOUBLE)
        .build();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  }

  /**
   * Build {@code numSegments} fresh segments of {@code rowsPerSegment} rows each. Anchor dims are drawn
   * independently at their fixed cardinality; {@link #DIM_HIGH} is drawn from a range >> rows so its
   * per-segment dictionary cardinality ~= {@code rowsPerSegment} (scales with the sweep).
   */
  private List<IndexSegment> buildSegments(int numSegments, int rowsPerSegment)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    List<IndexSegment> segments = new ArrayList<>(numSegments);
    for (int s = 0; s < numSegments; s++) {
      Random rng = new Random(20260723L + s);
      List<GenericRow> rows = new ArrayList<>(rowsPerSegment);
      for (int i = 0; i < rowsPerSegment; i++) {
        GenericRow row = new GenericRow();
        row.putValue(DIM_C32, rng.nextInt(CARD_C32));
        row.putValue(DIM_C100A, rng.nextInt(CARD_C100));
        row.putValue(DIM_C100B, rng.nextInt(CARD_C100));
        row.putValue(DIM_C100C, rng.nextInt(CARD_C100));
        row.putValue(DIM_C256, rng.nextInt(CARD_C256));
        row.putValue(DIM_C1000, rng.nextInt(CARD_C1000));
        row.putValue(DIM_STR1000, "str_" + rng.nextInt(CARD_C1000));   // <= 20 chars
        row.putValue(DIM_LONG1000, (long) rng.nextInt(CARD_C1000));
        row.putValue(DIM_HIGH, rng.nextInt(Integer.MAX_VALUE));        // range >> rows -> card ~= rows/seg
        row.putValue(INT_MET, rng.nextInt(2_000_000) - 1_000_000);
        row.putValue(FLOAT_MET, rng.nextFloat() * 2000.0f - 1000.0f);
        row.putValue(LONG_MET, (long) (rng.nextInt(2_000_000) - 1_000_000));
        row.putValue(DOUBLE_MET, rng.nextDouble() * 2000.0 - 1000.0);
        rows.add(row);
      }
      String segmentName = "seg_" + s;
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
      config.setTableName(RAW_TABLE_NAME);
      config.setSegmentName(segmentName);
      config.setOutDir(INDEX_DIR.getPath());
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(rows));
      driver.build();
      segments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap));
    }
    return segments;
  }

  private void destroySegments() {
    if (_segments != null) {
      for (IndexSegment segment : _segments) {
        segment.destroy();
      }
      _segments = null;
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    clearNative();
    destroySegments();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void sweep()
      throws Exception {
    int numSegments = Integer.getInteger("sweep.segments", 10);
    int[] sizes = parseInts(System.getProperty("sweep.sizes"), DEFAULT_SIZES);
    int[] limits = parseInts(System.getProperty("sweep.limits"), DEFAULT_LIMITS);
    String filter = System.getProperty("sweep.filter", "");

    // Each shape: {label, group/select keys, ORDER BY keys, expected segment tier}. The expected tier is
    // derived from the ladder gate (Sigma bits <= 20 -> T1; else product <= 2^20 -> T0; else T2) on the
    // FIXED anchor cardinalities, so it holds at every size. "single" = single-column path (dict-direct
    // dense, or hash once one column exceeds 2^20 distinct — only at very large sizes).
    // ORDER BY is always the full group key (unique per group) => deterministic top-K => exact row parity.
    String c256c1000 = DIM_C256 + ", " + DIM_C1000;
    String c32c100c256 = DIM_C32 + ", " + DIM_C100A + ", " + DIM_C256;
    String c100x3 = DIM_C100A + ", " + DIM_C100B + ", " + DIM_C100C;
    String c1000high = DIM_C1000 + ", " + DIM_HIGH;
    String[][] shapes = {
        {"S1 gb(str1000)", DIM_STR1000, DIM_STR1000, "single"},
        {"S2 gb(long1000)", DIM_LONG1000, DIM_LONG1000, "single"},
        {"S3 gb(high)", DIM_HIGH, DIM_HIGH, "single"},
        {"T1a gb(c256,c1000)", c256c1000, c256c1000, "T1"},
        {"T1b gb(c32,c100,c256)", c32c100c256, c32c100c256, "T1"},
        {"T0a gb(c100,c100,c100)", c100x3, c100x3, "T0"},
        {"T2a gb(c1000,high)", c1000high, c1000high, "T2"}
    };

    List<String> failures = new ArrayList<>();
    for (int totalRows : sizes) {
      int rowsPerSegment = Math.max(1, totalRows / numSegments);
      int actualTotal = rowsPerSegment * numSegments;
      _segments = buildSegments(numSegments, rowsPerSegment);
      int warmup = warmupIters(rowsPerSegment);
      int measure = measureIters(rowsPerSegment);

      System.out.println();
      System.out.printf("=== NativeGroupBySweepTest — %,d rows = %d segments x %,d "
              + "(dimHigh card ~= %,d/seg; warmup=%d measure=%d) ===%n",
          actualTotal, numSegments, rowsPerSegment, rowsPerSegment, warmup, measure);
      System.out.printf("%-24s %7s %8s %10s %10s %10s %10s %6s%n",
          "shape", "tier", "LIMIT", "baseMs", "nativeMs", "speedup", "rows", "match");
      System.out.println(
          "-----------------------------------------------------------------------------------------------");

      for (String[] shape : shapes) {
        String label = shape[0];
        String keys = shape[1];
        String orderKeys = shape[2];
        String tier = shape[3];
        if (!filter.isEmpty() && !label.contains(filter)) {
          continue;
        }
        String bodyBeforeLimit = "SELECT " + keys + ", " + aggList() + " FROM " + RAW_TABLE_NAME
            + " GROUP BY " + keys + " ORDER BY " + orderKeys;
        for (int limit : limits) {
          String query = bodyBeforeLimit + " LIMIT " + limit;

          ResultTable baseResult = getBrokerResponse(query, QUERY_OPTIONS).getResultTable();
          setNative();
          ResultTable nativeResult = getBrokerResponse(query, QUERY_OPTIONS_NATIVE).getResultTable();
          clearNative();
          int baseRows = baseResult.getRows().size();
          int nativeRows = nativeResult.getRows().size();
          boolean match = baseRows == nativeRows;
          if (!match) {
            failures.add(String.format("%s @ %,d rows LIMIT %d: base=%d native=%d",
                label, actualTotal, limit, baseRows, nativeRows));
          }

          double baseMs = time(query, false, warmup, measure);
          double nativeMs = time(query, true, warmup, measure);
          clearNative();

          System.out.printf("%-24s %7s %8d %10.4f %10.4f %9.2fx %,10d %6s%n",
              label, tier, limit, baseMs, nativeMs, baseMs / nativeMs, nativeRows, match);
        }
      }
      destroySegments();
    }
    System.out.println();
    org.testng.Assert.assertTrue(failures.isEmpty(), "row-count parity failures: " + failures);
  }

  private double time(String query, boolean nativePath, int warmup, int measure) {
    Map<String, String> options;
    if (nativePath) {
      setNative();
      options = QUERY_OPTIONS_NATIVE;
    } else {
      clearNative();
      options = QUERY_OPTIONS;
    }
    for (int i = 0; i < warmup; i++) {
      getBrokerResponse(query, options);
    }
    long start = System.nanoTime();
    for (int i = 0; i < measure; i++) {
      getBrokerResponse(query, options);
    }
    return (System.nanoTime() - start) / 1e6 / measure;
  }

  private static int warmupIters(int rowsPerSegment) {
    return rowsPerSegment <= 20_000 ? 20 : (rowsPerSegment <= 100_000 ? 5 : 2);
  }

  private static int measureIters(int rowsPerSegment) {
    return rowsPerSegment <= 20_000 ? 50 : (rowsPerSegment <= 100_000 ? 15 : 5);
  }

  private static int[] parseInts(String csv, int[] def) {
    if (csv == null || csv.isEmpty()) {
      return def;
    }
    String[] parts = csv.split(",");
    int[] out = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      out[i] = Integer.parseInt(parts[i].trim());
    }
    return out;
  }

  private static String aggList() {
    StringBuilder sb = new StringBuilder();
    for (String m : new String[]{INT_MET, FLOAT_MET, LONG_MET, DOUBLE_MET}) {
      sb.append("SUM(").append(m).append("), MIN(").append(m).append("), MAX(").append(m).append("), ");
    }
    sb.setLength(sb.length() - 2);
    return sb.toString();
  }

  private void setNative() {
    System.setProperty(SERIALIZE_PROPERTY, "true");
    // Multi-column combine merge strategy: ColumnWise (DataFusion-style typed column hashing, design B).
    System.setProperty(MULTICOL_PROPERTY, "columnwise");
  }

  private void clearNative() {
    System.clearProperty(SERIALIZE_PROPERTY);
    System.clearProperty(MULTICOL_PROPERTY);
  }

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
