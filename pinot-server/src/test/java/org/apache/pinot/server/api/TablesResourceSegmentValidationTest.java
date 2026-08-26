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
package org.apache.pinot.server.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests the server side of the hybrid time boundary flow: {@code GET /tables/{table}/allSegmentsLoaded}, backed by
 * {@link org.apache.pinot.server.api.resources.TablesResource#validateTableSegmentState}.
 * <p>
 * The controller refuses to advance a hybrid table's time boundary unless servers report their segments as loaded and
 * matching the CRC recorded in ZK, so these cases guard against silently serving stale or missing data.
 */
public class TablesResourceSegmentValidationTest extends BaseResourceTest {
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String VALIDATION_PATH = "/tables/" + OFFLINE_TABLE_NAME + "/allSegmentsLoaded";
  private static final String CLUSTER_NAME = "TestCluster";

  private final Map<String, ZNRecord> _segmentZKMetadata = new HashMap<>();
  private IdealState _idealState;

  @BeforeMethod
  public void setUpHelixMocks() {
    _segmentZKMetadata.clear();
    _idealState = new IdealState(OFFLINE_TABLE_NAME);

    // HelixHelper.getTableIdealState() reads the ideal state through the data accessor.
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);
    when(accessor.getProperty(any(PropertyKey.class))).thenAnswer(invocation -> _idealState);
    when(_helixManager.getHelixDataAccessor()).thenReturn(accessor);

    // ZKMetadataProvider.getSegmentZKMetadata() reads segment metadata through the property store.
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get(any(String.class), eq(null), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      String path = invocation.getArgument(0);
      return _segmentZKMetadata.get(path.substring(path.lastIndexOf('/') + 1));
    });
    when(_helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
  }

  /**
   * Assigns {@code segmentName} to this server in the ideal state with the given state.
   */
  private void assignToThisServer(String segmentName, String state) {
    _idealState.setPartitionState(segmentName, _instanceId, state);
  }

  private void assignToOtherServer(String segmentName, String state) {
    _idealState.setPartitionState(segmentName, "Server_other_host_1234", state);
  }

  /**
   * Records ZK metadata for a segment with the given CRC and end time.
   */
  private void setSegmentZKMetadata(String segmentName, long crc, long endTimeMs) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(segmentName);
    metadata.setCrc(crc);
    metadata.setStartTime(endTimeMs - 1);
    metadata.setEndTime(endTimeMs);
    metadata.setTimeUnit(java.util.concurrent.TimeUnit.MILLISECONDS);
    _segmentZKMetadata.put(segmentName, metadata.toZNRecord());
  }

  private TableSegmentValidationInfo callValidate() {
    return _webTarget.path(VALIDATION_PATH).request().get(TableSegmentValidationInfo.class);
  }

  private long crcOf(ImmutableSegment segment) {
    return Long.parseLong(segment.getSegmentMetadata().getCrc());
  }

  @Test
  public void testAllSegmentsLoadedAndCrcMatches() {
    // Every ONLINE segment is loaded with the CRC recorded in ZK, so the table is valid and the reported end time is
    // the maximum across the segments assigned to this server.
    List<ImmutableSegment> segments = _offlineIndexSegments;
    assertFalse(segments.isEmpty());
    long expectedMaxEndTimeMs = 0;
    for (int i = 0; i < segments.size(); i++) {
      ImmutableSegment segment = segments.get(i);
      long endTimeMs = 1000L * (i + 1);
      expectedMaxEndTimeMs = Math.max(expectedMaxEndTimeMs, endTimeMs);
      setSegmentZKMetadata(segment.getSegmentName(), crcOf(segment), endTimeMs);
      assignToThisServer(segment.getSegmentName(), SegmentStateModel.ONLINE);
    }
    assignToOtherServer("segment_hosted_elsewhere", SegmentStateModel.ONLINE);
    setSegmentZKMetadata("segment_offline_state", 7L, expectedMaxEndTimeMs + 10_000L);
    assignToThisServer("segment_offline_state", SegmentStateModel.OFFLINE);

    TableSegmentValidationInfo info = callValidate();
    assertTrue(info.isValid(), "Expected valid, got reason: " + info.getInvalidReason());
    assertEquals(info.getMaxEndTimeMs(), expectedMaxEndTimeMs);
  }

  @Test
  public void testCrcMismatchIsInvalid() {
    // A segment can be ONLINE in Helix while the server still holds an older copy, e.g. after a refresh push. The CRC
    // comparison is the only signal that catches this, and advancing the time boundary over it would serve stale data.
    ImmutableSegment segment = _offlineIndexSegments.get(0);
    setSegmentZKMetadata(segment.getSegmentName(), crcOf(segment) + 1, 5000L);
    assignToThisServer(segment.getSegmentName(), SegmentStateModel.ONLINE);

    TableSegmentValidationInfo info = callValidate();
    assertFalse(info.isValid());
    assertNotNull(info.getInvalidReason());
    assertTrue(info.getInvalidReason().contains("CRC mismatch"), "Got: " + info.getInvalidReason());
    assertTrue(info.getInvalidReason().contains(segment.getSegmentName()), "Got: " + info.getInvalidReason());
  }

  @Test
  public void testOnlineSegmentNotLoadedIsInvalid() {
    // The ideal state assigns a segment this server has never loaded.
    String missingSegment = "segment_not_loaded";
    setSegmentZKMetadata(missingSegment, 42L, 5000L);
    assignToThisServer(missingSegment, SegmentStateModel.ONLINE);

    TableSegmentValidationInfo info = callValidate();
    assertFalse(info.isValid());
    assertNotNull(info.getInvalidReason());
    assertTrue(info.getInvalidReason().contains(missingSegment), "Got: " + info.getInvalidReason());
  }

  @Test
  public void testConsumingSegmentNotLoadedIsInvalid() {
    assignToThisServer("segment_consuming_missing", SegmentStateModel.CONSUMING);

    TableSegmentValidationInfo info = callValidate();
    assertFalse(info.isValid());
    assertNotNull(info.getInvalidReason());
    assertTrue(info.getInvalidReason().contains("CONSUMING"), "Got: " + info.getInvalidReason());
  }

  @Test
  public void testNoSegmentsAssignedReportsNoEndTime() {
    // With nothing assigned there is no end time to report. The controller turns this into a failure rather than
    // writing a negative time boundary.
    TableSegmentValidationInfo info = callValidate();
    assertTrue(info.isValid(), "Expected valid, got reason: " + info.getInvalidReason());
    assertEquals(info.getMaxEndTimeMs(), -1L);
  }
}
