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
package org.apache.pinot.core.query.aggregation.function;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for the native (Rust+JNI) aggregation path.
 *
 * <p>The test bootstraps {@code PinotNativeAgg} by setting {@code pinot.native.lib.path} in
 * a static block <em>before</em> any reference to the class is made, so the library is loaded
 * from the dev-build location ({@code ../pinot-native/native/target/release/libpinot_native.*}).
 * If the library can't be found, the entire suite is skipped — running this test requires
 * {@code mvn -pl pinot-native package} to have produced the binary.
 */
public class NativeSumAggregationFunctionTest {

  private static final String LIB_PATH_PROP = "pinot.native.lib.path";

  // Set the lib path system property before PinotNativeAgg is touched anywhere in this test.
  // Static blocks of a class run when the class is first loaded.
  static {
    String resolved = resolveDevLibPath();
    if (resolved != null && System.getProperty(LIB_PATH_PROP) == null) {
      System.setProperty(LIB_PATH_PROP, resolved);
    }
  }

  @BeforeClass
  public void checkNativeAvailable() {
    if (!PinotNativeAgg.isAvailable()) {
      throw new SkipException("pinot-native library not loadable. Build it with "
          + "'./mvnw -pl pinot-native package' first. Searched at "
          + System.getProperty(LIB_PATH_PROP));
    }
  }

  @Test
  public void factoryReturnsNativeImplWhenFlagOnAndEligible() {
    FunctionContext fc = sumOfColumn("longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false, true);
    assertTrue(fn instanceof NativeSumAggregationFunction,
        "expected NativeSumAggregationFunction, got " + fn.getClass().getName());
  }

  @Test
  public void factoryReturnsJavaImplWhenFlagOff() {
    FunctionContext fc = sumOfColumn("longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false, false);
    assertEquals(fn.getClass(), SumAggregationFunction.class,
        "expected plain SumAggregationFunction when flag is off, got " + fn.getClass().getName());
  }

  @Test
  public void factoryReturnsJavaImplWhenNullHandlingEnabled() {
    FunctionContext fc = sumOfColumn("longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, true, true);
    assertEquals(fn.getClass(), SumAggregationFunction.class,
        "null handling currently disqualifies the native path");
  }

  @Test
  public void aggregateLongMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }

    double nativeResult = runAggregate(new NativeSumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("longCol")), false),
        values, DataType.LONG, n);
    double javaResult = runAggregate(new SumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("longCol")), false),
        values, DataType.LONG, n);

    double tolerance = Math.max(1.0, Math.abs(javaResult) * 1e-15);
    assertTrue(Math.abs(nativeResult - javaResult) <= tolerance,
        "native=" + nativeResult + " java=" + javaResult);
  }

  @Test
  public void aggregateIntMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = (rng.nextInt() % 1_000_000);
    }
    double nativeResult = runAggregateInt(new NativeSumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("intCol")), false), values, n);
    double javaResult = runAggregateInt(new SumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("intCol")), false), values, n);
    double tolerance = Math.max(1.0, Math.abs(javaResult) * 1e-15);
    assertTrue(Math.abs(nativeResult - javaResult) <= tolerance,
        "native=" + nativeResult + " java=" + javaResult);
  }

  @Test
  public void aggregateFloatMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    double nativeResult = runAggregateFloat(new NativeSumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("floatCol")), false), values, n);
    double javaResult = runAggregateFloat(new SumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("floatCol")), false), values, n);
    double tolerance = Math.max(1.0, Math.abs(javaResult) * 1e-12);
    assertTrue(Math.abs(nativeResult - javaResult) <= tolerance,
        "native=" + nativeResult + " java=" + javaResult);
  }

  @Test
  public void aggregateDoubleMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double nativeResult = runAggregateDouble(new NativeSumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("doubleCol")), false), values, n);
    double javaResult = runAggregateDouble(new SumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("doubleCol")), false), values, n);
    double tolerance = Math.max(1.0, Math.abs(javaResult) * 1e-12);
    assertTrue(Math.abs(nativeResult - javaResult) <= tolerance,
        "native=" + nativeResult + " java=" + javaResult);
  }

  // --- helpers ----------------------------------------------------------------

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static double runAggregate(SumAggregationFunction fn, long[] values, DataType type,
      int length) {
    NativePrimitiveBlockValSet bvs = NativePrimitiveBlockValSet.forLong(values);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static double runAggregateInt(SumAggregationFunction fn, int[] values, int length) {
    NativePrimitiveBlockValSet bvs = NativePrimitiveBlockValSet.forInt(values);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static double runAggregateFloat(SumAggregationFunction fn, float[] values, int length) {
    NativePrimitiveBlockValSet bvs = NativePrimitiveBlockValSet.forFloat(values);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static double runAggregateDouble(SumAggregationFunction fn, double[] values, int length) {
    NativePrimitiveBlockValSet bvs = NativePrimitiveBlockValSet.forDouble(values);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  private static FunctionContext sumOfColumn(String column) {
    return new FunctionContext(FunctionContext.Type.AGGREGATION, "SUM",
        Collections.singletonList(ExpressionContext.forIdentifier(column)));
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
    // Surefire CWD for a module test is the module root (pinot-core). The sibling pinot-native
    // module hosts the Cargo build output.
    Path candidate =
        Paths.get("..", "pinot-native", "native", "target", "release", libFile).toAbsolutePath();
    if (Files.exists(candidate)) {
      return candidate.toString();
    }
    return null;
  }
}
