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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeInvertedIndexSizeTest {

  @Test
  public void testInvertedIndexSizeGrowsWithData() {
    RealtimeInvertedIndex index = new RealtimeInvertedIndex();
    assertEquals(index.getOnHeapSizeEstimateBytes(), 0);

    index.add(0, 0);
    long oneBitmapSize = index.getOnHeapSizeEstimateBytes();
    assertTrue(oneBitmapSize > 0);

    index.add(1, 1);
    long twoBitmapsSize = index.getOnHeapSizeEstimateBytes();
    assertEquals(twoBitmapsSize, 2 * oneBitmapSize);

    // Spread doc ids across several roaring containers
    for (int docId = 2; docId < 500_000; docId += 1000) {
      index.add(0, docId);
    }
    assertTrue(index.getOnHeapSizeEstimateBytes() > twoBitmapsSize);
  }

  @Test
  public void testNullVectorSize() {
    MutableNullValueVector nullVector = new MutableNullValueVector();
    long emptySize = nullVector.getOnHeapSizeEstimateBytes();
    for (int docId = 0; docId < 100_000; docId += 7) {
      nullVector.setNull(docId);
    }
    assertTrue(nullVector.getOnHeapSizeEstimateBytes() > emptySize);
  }

  @Test
  public void testOnHeapDictionarySize()
      throws Exception {
    try (StringOnHeapMutableDictionary dictionary = new StringOnHeapMutableDictionary()) {
      assertEquals(dictionary.getOnHeapSizeEstimateBytes(), 0);
      dictionary.index("short");
      long oneValue = dictionary.getOnHeapSizeEstimateBytes();
      assertTrue(oneValue > 0);
      dictionary.index("short");
      assertEquals(dictionary.getOnHeapSizeEstimateBytes(), oneValue);
      dictionary.index("a considerably longer dictionary value");
      assertTrue(dictionary.getOnHeapSizeEstimateBytes() > 2 * oneValue);
    }
  }

  @Test
  public void testOffHeapDictionaryOnlyCountsOverflow()
      throws Exception {
    try (DirectMemoryManager memoryManager = new DirectMemoryManager("test");
        IntOffHeapMutableDictionary dictionary = new IntOffHeapMutableDictionary(100, 0, memoryManager, "col")) {
      for (int i = 0; i < 50; i++) {
        dictionary.index(i);
      }
      assertEquals(dictionary.getOnHeapSizeEstimateBytes(), 0);
    }
  }
}
