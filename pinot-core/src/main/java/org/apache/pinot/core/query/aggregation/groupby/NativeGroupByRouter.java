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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Decides whether a segment-level GROUP BY can run on the native (Rust+JNI) path via
 * {@link NativeGroupByExecutor}. Mirrors {@code NativeAggregationRouter} for the group-by case.
 *
 * <p>Eligibility (all must hold):
 * <ol>
 *   <li>The native engine is enabled for this query (resolved from table/cluster config,
 *       {@code queryContext.isNativeEngineEnabled()}).</li>
 *   <li>The native library is loaded ({@link PinotNativeGroupBy#isAvailable()}).</li>
 *   <li>Null handling is disabled.</li>
 *   <li>No in-segment trim is triggered (no ORDER BY with a positive min segment group trim size) —
 *       the native executor does not implement trim yet.</li>
 *   <li>One or more group-by keys, each single-value, dict-encoded, of a fixed-width type
 *       (INT/LONG/FLOAT/DOUBLE). A single key uses the dict-id driver directly; multiple keys pack
 *       their dict-ids into one i64 key, which requires the per-column bit widths
 *       ({@code ceil(log2(cardinality))}) to sum to {@code <= 64} (design §17.9). Dict ids are the
 *       native key; the key's logical type only affects final decode.</li>
 *   <li>Every aggregation is SUM/MIN/MAX/COUNT. SUM/MIN/MAX take a single simple column identifier
 *       that is single-value and fixed-width; COUNT takes no column constraint.</li>
 * </ol>
 *
 * <p>The router never throws; when ineligible the caller uses the Java executor unchanged.
 */
public final class NativeGroupByRouter {

  private NativeGroupByRouter() {
  }

  @SuppressWarnings("rawtypes")
  public static boolean shouldAccelerate(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      BaseProjectOperator<?> projectOperator) {
    if (!queryContext.isNativeEngineEnabled()) {
      return false;
    }
    if (!PinotNativeGroupBy.isAvailable()) {
      return false;
    }
    if (queryContext.isNullHandlingEnabled()) {
      return false;
    }
    // The native executor cannot trim yet; decline when the in-segment trim path would trigger.
    if (queryContext.getOrderByExpressions() != null && queryContext.getMinSegmentGroupTrimSize() > 0) {
      return false;
    }
    // One or more dict-encoded, single-value keys. Keys are grouped on their fixed-width i32 dict-ids
    // (type-uniform at segment, design §25.5), so STRING/BYTES dict keys are eligible too — decoded to
    // the raw value at the drain boundary via Dictionary.getInternal. For multi-column, the packed key
    // (per-column ceil(log2(cardinality)) bits) must fit in 64 bits (design §17.9).
    if (groupByExpressions.length < 1) {
      return false;
    }
    long totalKeyBits = 0;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      ColumnContext keyContext = projectOperator.getResultColumnContext(groupByExpression);
      Dictionary keyDictionary = keyContext.getDictionary();
      if (!keyContext.isSingleValue() || keyDictionary == null
          || !isSupportedKeyType(keyContext.getDataType().getStoredType())) {
        return false;
      }
      totalKeyBits += bitsFor(keyDictionary.length());
    }
    if (totalKeyBits > 64) {
      return false;
    }
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    if (aggregationFunctions == null || aggregationFunctions.length == 0) {
      return false;
    }
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (!isSupportedAggregation(aggregationFunction, projectOperator)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  private static boolean isSupportedAggregation(AggregationFunction aggregationFunction,
      BaseProjectOperator<?> projectOperator) {
    AggregationFunctionType type = aggregationFunction.getType();
    if (type == AggregationFunctionType.COUNT) {
      return true;
    }
    if (type != AggregationFunctionType.SUM && type != AggregationFunctionType.MIN
        && type != AggregationFunctionType.MAX) {
      return false;
    }
    List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
    if (inputExpressions.size() != 1) {
      return false;
    }
    ExpressionContext inputExpression = inputExpressions.get(0);
    if (inputExpression.getType() != ExpressionContext.Type.IDENTIFIER) {
      return false;
    }
    ColumnContext valueContext = projectOperator.getResultColumnContext(inputExpression);
    return valueContext.isSingleValue() && isFixedWidth(valueContext.getDataType().getStoredType());
  }

  private static boolean isFixedWidth(DataType storedType) {
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Eligible native segment group-by <b>key</b> types. Keys are grouped on their fixed-width i32
   * dict-ids (type-uniform, design §25.5), so this accepts the fixed-width types plus dict-encoded
   * STRING/BYTES (decoded to the raw value at the drain boundary via {@link Dictionary#getInternal},
   * fed to the combine's arena key surface). BIG_DECIMAL and others are excluded — no combine key
   * surface for them. Distinct from {@link #isFixedWidth}, which still gates aggregation <b>value</b>
   * columns (SUM/MIN/MAX read raw fixed-width values).
   */
  private static boolean isSupportedKeyType(DataType storedType) {
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
        return true;
      default:
        return false;
    }
  }

  /**
   * Bits needed to represent dict_ids in {@code [0, cardinality)} — mirrors the native
   * {@code multi_key::bits_for}. A cardinality {@code <= 1} needs 0 bits (only id 0 exists).
   */
  private static long bitsFor(int cardinality) {
    if (cardinality <= 1) {
      return 0;
    }
    return 64 - Long.numberOfLeadingZeros((long) (cardinality - 1));
  }
}
