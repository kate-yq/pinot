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
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration tests for {@link NativeMinAggregationFunction} and
 * {@link NativeMaxAggregationFunction} — verifies factory routing, per-type dispatch
 * to {@link PinotNativeAgg}, and NaN propagation parity with the Java parents.
 */
public class NativeMinMaxAggregationFunctionTest {

  private static final String LIB_PATH_PROP = "pinot.native.lib.path";

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
          + "'./mvnw -pl pinot-native package' first.");
    }
  }

  // --- factory routing -----------------------------------------------------

  @Test
  public void factoryReturnsNativeMinWhenEligible() {
    FunctionContext fc = function("MIN", "longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false, true);
    assertTrue(fn instanceof NativeMinAggregationFunction,
        "expected NativeMinAggregationFunction, got " + fn.getClass().getName());
  }

  @Test
  public void factoryReturnsNativeMaxWhenEligible() {
    FunctionContext fc = function("MAX", "doubleCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false, true);
    assertTrue(fn instanceof NativeMaxAggregationFunction,
        "expected NativeMaxAggregationFunction, got " + fn.getClass().getName());
  }

  @Test
  public void factoryFallsBackToJavaForMinWhenNullHandlingEnabled() {
    FunctionContext fc = function("MIN", "longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, true, true);
    assertEquals(fn.getClass(), MinAggregationFunction.class);
  }

  // --- MIN per-type --------------------------------------------------------

  @Test
  public void minIntMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextInt() % 1_000_000;
    }
    double nativeResult = runMinInt(new NativeMinAggregationFunction(idArg("intCol"), false), values, n);
    double javaResult = runMinInt(new MinAggregationFunction(idArg("intCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  @Test
  public void minLongMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }
    double nativeResult = runMinLong(new NativeMinAggregationFunction(idArg("longCol"), false), values, n);
    double javaResult = runMinLong(new MinAggregationFunction(idArg("longCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  @Test
  public void minFloatMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    double nativeResult = runMinFloat(new NativeMinAggregationFunction(idArg("floatCol"), false), values, n);
    double javaResult = runMinFloat(new MinAggregationFunction(idArg("floatCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  @Test
  public void minDoubleMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double nativeResult = runMinDouble(new NativeMinAggregationFunction(idArg("doubleCol"), false), values, n);
    double javaResult = runMinDouble(new MinAggregationFunction(idArg("doubleCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  // --- MAX per-type --------------------------------------------------------

  @Test
  public void maxIntMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextInt() % 1_000_000;
    }
    double nativeResult = runMaxInt(new NativeMaxAggregationFunction(idArg("intCol"), false), values, n);
    double javaResult = runMaxInt(new MaxAggregationFunction(idArg("intCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  @Test
  public void maxLongMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }
    double nativeResult = runMaxLong(new NativeMaxAggregationFunction(idArg("longCol"), false), values, n);
    double javaResult = runMaxLong(new MaxAggregationFunction(idArg("longCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  @Test
  public void maxFloatMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    double nativeResult = runMaxFloat(new NativeMaxAggregationFunction(idArg("floatCol"), false), values, n);
    double javaResult = runMaxFloat(new MaxAggregationFunction(idArg("floatCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  @Test
  public void maxDoubleMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double nativeResult = runMaxDouble(new NativeMaxAggregationFunction(idArg("doubleCol"), false), values, n);
    double javaResult = runMaxDouble(new MaxAggregationFunction(idArg("doubleCol"), false), values, n);
    assertEquals(nativeResult, javaResult);
  }

  // --- NaN parity ---------------------------------------------------------

  @Test
  public void minDoubleNanPropagatesViaJavaFallback() {
    // The kernel returns NaN, which the native impl can't distinguish from a JNI error,
    // so it falls through to the Java parent. Java's path also produces NaN. End result
    // is identical either way; this test pins the externally-visible behavior.
    double[] values = {1.0, 2.0, Double.NaN, 3.0};
    double nativeResult = runMinDouble(new NativeMinAggregationFunction(idArg("doubleCol"), false), values, 4);
    assertTrue(Double.isNaN(nativeResult), "expected NaN, got " + nativeResult);
  }

  @Test
  public void maxFloatNanPropagatesViaJavaFallback() {
    float[] values = {1.0f, 2.0f, Float.NaN, 3.0f};
    double nativeResult = runMaxFloat(new NativeMaxAggregationFunction(idArg("floatCol"), false), values, 4);
    assertTrue(Double.isNaN(nativeResult), "expected NaN, got " + nativeResult);
  }

  // --- helpers ----------------------------------------------------------------

  private static double runMinInt(MinAggregationFunction fn, int[] values, int length) {
    return runMin(fn, NativePrimitiveBlockValSet.forInt(values), length);
  }

  private static double runMinLong(MinAggregationFunction fn, long[] values, int length) {
    return runMin(fn, NativePrimitiveBlockValSet.forLong(values), length);
  }

  private static double runMinFloat(MinAggregationFunction fn, float[] values, int length) {
    return runMin(fn, NativePrimitiveBlockValSet.forFloat(values), length);
  }

  private static double runMinDouble(MinAggregationFunction fn, double[] values, int length) {
    return runMin(fn, NativePrimitiveBlockValSet.forDouble(values), length);
  }

  private static double runMaxInt(MaxAggregationFunction fn, int[] values, int length) {
    return runMax(fn, NativePrimitiveBlockValSet.forInt(values), length);
  }

  private static double runMaxLong(MaxAggregationFunction fn, long[] values, int length) {
    return runMax(fn, NativePrimitiveBlockValSet.forLong(values), length);
  }

  private static double runMaxFloat(MaxAggregationFunction fn, float[] values, int length) {
    return runMax(fn, NativePrimitiveBlockValSet.forFloat(values), length);
  }

  private static double runMaxDouble(MaxAggregationFunction fn, double[] values, int length) {
    return runMax(fn, NativePrimitiveBlockValSet.forDouble(values), length);
  }

  private static double runMin(MinAggregationFunction fn, NativePrimitiveBlockValSet bvs,
      int length) {
    AggregationResultHolder holder = new DoubleAggregationResultHolder(Double.POSITIVE_INFINITY);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  private static double runMax(MaxAggregationFunction fn, NativePrimitiveBlockValSet bvs,
      int length) {
    AggregationResultHolder holder = new DoubleAggregationResultHolder(Double.NEGATIVE_INFINITY);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  private static FunctionContext function(String op, String column) {
    return new FunctionContext(FunctionContext.Type.AGGREGATION, op,
        Collections.singletonList(ExpressionContext.forIdentifier(column)));
  }

  private static java.util.List<ExpressionContext> idArg(String column) {
    return Collections.singletonList(ExpressionContext.forIdentifier(column));
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
    Path candidate =
        Paths.get("..", "pinot-native", "native", "target", "release", libFile).toAbsolutePath();
    return Files.exists(candidate) ? candidate.toString() : null;
  }
}
