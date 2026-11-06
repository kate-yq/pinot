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
package org.apache.pinot.segment.local.realtime.impl.dictionary;

import java.math.BigDecimal;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Rough estimates of the JVM heap retained by dictionary entries (assuming compressed oops). These are approximations
 * meant for capacity planning, not exact measurements.
 */
final class DictionaryHeapSizeEstimator {
  private DictionaryHeapSizeEstimator() {
  }

  // ConcurrentHashMap node (32) + boxed Integer dictId (16) + slot in the table array and in the dictId->value array
  static final long MAP_ENTRY_OVERHEAD_BYTES = 32 + 16 + 4 + 4;
  private static final long BOXED_PRIMITIVE_BYTES = 16;
  private static final long STRING_OVERHEAD_BYTES = 24 + 16;
  private static final long BYTE_ARRAY_OVERHEAD_BYTES = 16 + 16;
  private static final long BIG_DECIMAL_BYTES = 40 + 16 + 24;

  static long estimateValueSize(Object value) {
    if (value instanceof String) {
      // Latin1 strings use 1 byte per char, others 2; use the UTF-16 upper bound
      return STRING_OVERHEAD_BYTES + 2L * ((String) value).length();
    }
    if (value instanceof ByteArray) {
      return BYTE_ARRAY_OVERHEAD_BYTES + ((ByteArray) value).length();
    }
    if (value instanceof BigDecimal) {
      return BIG_DECIMAL_BYTES;
    }
    return BOXED_PRIMITIVE_BYTES;
  }
}
