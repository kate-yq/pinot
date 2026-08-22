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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupByCombine;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Decides whether the server-level GROUP BY combine can run on the native (Rust+JNI) path via
 * {@link NativeGroupByCombineOperator}. Mirrors {@code NativeGroupByRouter} (segment level) for the
 * combine layer.
 *
 * <p>Eligibility:
 * <ol>
 *   <li>The native engine is enabled for this query ({@code queryContext.isNativeEngineEnabled()},
 *       resolved from table/cluster config).</li>
 *   <li>The native library is loaded.</li>
 *   <li>Null handling is disabled.</li>
 *   <li>One or more group-by keys. The operator dispatches per column on the type discovered from the
 *       first segment's {@code DataSchema}: INT/LONG → LONG, FLOAT/DOUBLE → DOUBLE, STRING/BYTES →
 *       arena. A single key uses the specialized single-key surface; multiple keys use the
 *       multi-column combine (raw-value tuple key, design §17.9 / §23.1).</li>
 *   <li>Every aggregation is SUM/MIN/MAX/COUNT (merged in {@code double} at combine).</li>
 *   <li>No HAVING (kept-group count would differ; deferred).</li>
 *   <li>No in-segment trim ({@code minSegmentGroupTrimSize <= 0}) — else the segment hands back a
 *       sorted {@code Collection<IntermediateRecord>} (with {@code AggregationGroupByResult == null}),
 *       a feed shape the operator does not consume (design §26.3/§26.7).</li>
 *   <li>ORDER BY is accepted when <b>every</b> order term resolves to the group-by key column or an
 *       aggregation-result column (native computes an exact top-K, §26.7); it is rejected for literal,
 *       non-group-by identifier, filtered-aggregation, or post-aggregation order keys — those fall back
 *       to the Java combine. No ORDER BY is always accepted (native applies the LIMIT cap).</li>
 * </ol>
 *
 * <p>Never throws; when ineligible the caller uses {@code GroupByCombineOperator} unchanged.
 */
public final class NativeGroupByCombineRouter {

  private NativeGroupByCombineRouter() {
  }

  /**
   * Whether the native combine path is enabled for this query by the resolved table/cluster gate
   * (does not check per-query eligibility).
   */
  public static boolean isEnabled(QueryContext queryContext) {
    return queryContext.isNativeEngineEnabled();
  }

  @SuppressWarnings("rawtypes")
  public static boolean shouldAccelerate(QueryContext queryContext) {
    if (!queryContext.isNativeEngineEnabled()) {
      return false;
    }
    if (!PinotNativeGroupByCombine.isAvailable()) {
      return false;
    }
    if (queryContext.isNullHandlingEnabled()) {
      return false;
    }
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions == null || groupByExpressions.isEmpty()) {
      return false;
    }
    // HAVING changes the kept-group count / result semantics — deferred.
    if (queryContext.getHavingFilter() != null) {
      return false;
    }
    // In-segment trim (ORDER BY + minSegmentGroupTrimSize > 0) makes the segment return sorted
    // IntermediateRecords instead of AggregationGroupByResult — a feed shape we do not consume.
    if (queryContext.getMinSegmentGroupTrimSize() > 0) {
      return false;
    }
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    if (aggregationFunctions == null || aggregationFunctions.length == 0) {
      return false;
    }
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      AggregationFunctionType type = aggregationFunction.getType();
      if (type != AggregationFunctionType.SUM && type != AggregationFunctionType.MIN
          && type != AggregationFunctionType.MAX && type != AggregationFunctionType.COUNT) {
        return false;
      }
    }
    // ORDER BY must resolve entirely to group-by key / aggregation columns (null => unsupported term).
    return NativeGroupByCombineOperator.resolveOrderSpec(queryContext) != null;
  }
}
