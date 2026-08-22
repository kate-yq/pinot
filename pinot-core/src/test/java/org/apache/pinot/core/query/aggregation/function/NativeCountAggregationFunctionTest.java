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
 * Integration tests for {@link NativeCountAggregationFunction} — verifies factory
 * routing and the no-JNI fast path for {@code COUNT(*)}.
 *
 * <p>The native COUNT has no kernel: it just adds the block length to the result
 * holder directly. So the only correctness assertion we can make is that the result
 * holder receives {@code length} per block.
 */
public class NativeCountAggregationFunctionTest {

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

  @Test
  public void factoryReturnsNativeCountForCountStar() {
    FunctionContext fc = new FunctionContext(FunctionContext.Type.AGGREGATION, "COUNT",
        Collections.singletonList(ExpressionContext.forIdentifier("*")));
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false, true);
    assertTrue(fn instanceof NativeCountAggregationFunction,
        "expected NativeCountAggregationFunction, got " + fn.getClass().getName());
  }

  @Test
  public void factoryReturnsNativeCountForCountColumn() {
    FunctionContext fc = new FunctionContext(FunctionContext.Type.AGGREGATION, "COUNT",
        Collections.singletonList(ExpressionContext.forIdentifier("col")));
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false, true);
    assertTrue(fn instanceof NativeCountAggregationFunction,
        "expected NativeCountAggregationFunction, got " + fn.getClass().getName());
  }

  @Test
  public void factoryFallsBackToJavaWhenNullHandlingEnabled() {
    FunctionContext fc = new FunctionContext(FunctionContext.Type.AGGREGATION, "COUNT",
        Collections.singletonList(ExpressionContext.forIdentifier("col")));
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, true, true);
    assertEquals(fn.getClass(), CountAggregationFunction.class);
  }

  @Test
  public void countStarAggregateAddsLengthDirectly() {
    NativeCountAggregationFunction fn = new NativeCountAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("*")), false);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    // COUNT(*) is contractually called with an empty blockValSetMap.
    fn.aggregate(10_000, holder, Collections.emptyMap());
    assertEquals(holder.getDoubleResult(), 10_000.0);
  }

  @Test
  public void countStarAggregateAccumulatesAcrossBlocks() {
    NativeCountAggregationFunction fn = new NativeCountAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("*")), false);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    fn.aggregate(10_000, holder, Collections.emptyMap());
    fn.aggregate(7_500, holder, Collections.emptyMap());
    fn.aggregate(3, holder, Collections.emptyMap());
    assertEquals(holder.getDoubleResult(), 17_503.0);
  }

  @Test
  public void countStarAggregateZeroLengthIsNoOp() {
    NativeCountAggregationFunction fn = new NativeCountAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("*")), false);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(42.0);
    fn.aggregate(0, holder, Collections.emptyMap());
    assertEquals(holder.getDoubleResult(), 42.0);
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
