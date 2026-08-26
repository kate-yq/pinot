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
package org.apache.pinot.controller.api.resources;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.core.Response;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Tests the controller fan-out and safety rules used before advancing a hybrid table's explicit time boundary.
 */
@Test(groups = "stateless")
public class PinotTableTimeBoundaryValidationTest {
  private static final String OFFLINE_TABLE_NAME = "myTable_OFFLINE";
  private static final String URI_PATH = "/tables/";
  private static final String SERVER_0 = "Server_localhost_0";
  private static final String SERVER_1 = "Server_localhost_1";
  private static final String SERVER_2 = "Server_localhost_2";
  private static final String SEGMENT_0 = "segment_0";
  private static final String SEGMENT_1 = "segment_1";

  private final ExecutorService _executor = Executors.newFixedThreadPool(3);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final Map<String, FakeHttpServer> _servers = new HashMap<>();

  private PinotHelixResourceManager _helix;
  private HybridTimeBoundaryValidator _validator;

  @BeforeMethod
  public void setUp() {
    _helix = mock(PinotHelixResourceManager.class);
    _validator = new HybridTimeBoundaryValidator(_helix, _executor, _connectionManager);
    setReplication(1);
  }

  @AfterMethod
  public void tearDown() {
    for (FakeHttpServer server : _servers.values()) {
      server.stop();
    }
    _servers.clear();
  }

  @AfterClass
  public void tearDownClass() {
    _executor.shutdownNow();
    _connectionManager.close();
  }

  @Test
  public void testAllServersValid()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(true, null, 3_000L));
    wireHelix(serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_1)),
        Arrays.asList(SERVER_0, SERVER_1));

    // This also proves both URL-keyed responses are attributed to the right server: otherwise the per-segment
    // coverage check fails because each server solely hosts one segment.
    assertEquals(_validator.validate(OFFLINE_TABLE_NAME).getMaxEndTimeMs(), 3_000L);
  }

  @Test(dataProvider = "nonRoutableServerStates")
  public void testIgnoresNonRoutableServer(String state)
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 2_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(true, null, 9_000L));
    wireHelix(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0, SEGMENT_1), SERVER_1, Arrays.asList(SEGMENT_0, SEGMENT_1)),
        Arrays.asList(SERVER_0, SERVER_1));
    configureNonRoutableState(SERVER_1, state);

    // The ignored server neither blocks nor contributes its higher end time.
    assertEquals(_validator.validate(OFFLINE_TABLE_NAME).getMaxEndTimeMs(), 2_000L);
  }

  @Test
  public void testRejectsRoutableNonResponder()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    wireHelix(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0, SEGMENT_1), SERVER_1, Arrays.asList(SEGMENT_0, SEGMENT_1)),
        Arrays.asList(SERVER_0, SERVER_1));

    ControllerApplicationException exception =
        expectFailure("Expected a routable non-responder to block");
    assertEquals(exception.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    assertTrue(exception.getMessage().contains(SERVER_1));
  }

  @Test
  public void testRejectsRoutableReplicaMissingFromExternalView()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(true, null, 1_000L));
    wireHelix(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0)),
        Arrays.asList(SERVER_0, SERVER_1));
    ExternalView externalView = _helix.getTableExternalView(OFFLINE_TABLE_NAME);
    externalView.getRecord().getMapField(SEGMENT_0).remove(SERVER_1);

    ControllerApplicationException exception =
        expectFailure("Expected a routable missing replica to block even when its server responds valid");
    assertEquals(exception.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    assertTrue(exception.getMessage().contains("may still be treated as new"));
  }

  @Test
  public void testRequiresCoverageForEverySegment()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(true, null, 2_000L));
    wireHelix(serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_1)),
        Arrays.asList(SERVER_0));

    // Even a valid response cannot provide coverage when brokers cannot route to that server.
    ControllerApplicationException exception =
        expectFailure("Expected unvalidated segment coverage to block");
    assertEquals(exception.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    assertTrue(exception.getMessage().contains(SEGMENT_1));
  }

  @Test
  public void testIgnoresServerWhoseSegmentsAreAllError()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(false, "Segment segment_0 is not loaded", -1L));
    wireHelix(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0)),
        Arrays.asList(SERVER_0, SERVER_1));
    ExternalView externalView = externalView(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0)));
    externalView.setState(SEGMENT_0, SERVER_1, SegmentStateModel.ERROR);
    when(_helix.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);

    assertEquals(_validator.validate(OFFLINE_TABLE_NAME).getMaxEndTimeMs(), 1_000L);
  }

  @Test
  public void testRequiresTwoServingReplicasWhenConfigured()
      throws Exception {
    setReplication(2);
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(false, "Segment segment_0 is not loaded", -1L));
    wireHelix(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0)),
        Arrays.asList(SERVER_0, SERVER_1));
    ExternalView externalView = externalView(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0)));
    externalView.setState(SEGMENT_0, SERVER_1, SegmentStateModel.ERROR);
    when(_helix.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);

    ControllerApplicationException exception =
        expectFailure("Expected RF=2 to retain two serving replicas");
    assertEquals(exception.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    assertTrue(exception.getMessage().contains("fewer than 2"));
  }

  @Test
  public void testAllowsOneUnavailableReplicaWhenThreeAreConfigured()
      throws Exception {
    setReplication(3);
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_2, new TableSegmentValidationInfo(false, "Segment segment_0 is not loaded", -1L));
    wireHelix(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0), SERVER_2,
            Arrays.asList(SEGMENT_0)),
        Arrays.asList(SERVER_0, SERVER_1, SERVER_2));
    ExternalView externalView = externalView(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0), SERVER_2,
            Arrays.asList(SEGMENT_0)));
    externalView.setState(SEGMENT_0, SERVER_2, SegmentStateModel.ERROR);
    when(_helix.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);

    assertEquals(_validator.validate(OFFLINE_TABLE_NAME).getMaxEndTimeMs(), 1_000L);
  }

  @Test
  public void testInvalidServerResponseIsRejected()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, new TableSegmentValidationInfo(false, "Segment segment_1 has CRC mismatch", -1L));
    wireHelix(serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_1)),
        Arrays.asList(SERVER_0, SERVER_1));

    ControllerApplicationException exception = expectFailure("Expected invalid segment state to block");
    assertEquals(exception.getResponse().getStatus(), Response.Status.PRECONDITION_FAILED.getStatusCode());
    assertTrue(exception.getMessage().contains(SERVER_1));
    assertTrue(exception.getMessage().contains("CRC mismatch"));
  }

  @Test
  public void testValidationOutcomeDetectsAssignmentChange()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    Map<String, List<String>> initial =
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_0));
    wireHelix(initial, Arrays.asList(SERVER_0));
    HybridTimeBoundaryValidator.ValidationOutcome outcome = _validator.validate(OFFLINE_TABLE_NAME);

    assertTrue(outcome.matchesAssignment(idealState(initial)));
    assertFalse(outcome.matchesAssignment(idealState(
        serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0, SEGMENT_1), SERVER_1, Arrays.asList(SEGMENT_0)))));
  }

  @Test
  public void testValidationOutcomeUsesVersionedIdealStateUpdate()
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    Map<String, List<String>> assignment = serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0));
    wireHelix(assignment, Arrays.asList(SERVER_0));
    HybridTimeBoundaryValidator.ValidationOutcome outcome = _validator.validate(OFFLINE_TABLE_NAME);

    IdealState currentIdealState = idealState(assignment);
    currentIdealState.getRecord().setVersion(7);
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    PropertyKey idealStateKey = mock(PropertyKey.class);
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    when(helixManager.getHelixDataAccessor()).thenReturn(dataAccessor);
    when(dataAccessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.idealStates(OFFLINE_TABLE_NAME)).thenReturn(idealStateKey);
    when(idealStateKey.getPath()).thenReturn("/IDEALSTATES/" + OFFLINE_TABLE_NAME);
    when(dataAccessor.getProperty(idealStateKey)).thenReturn(currentIdealState);
    when(dataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(baseDataAccessor.set(eq(idealStateKey.getPath()), any(ZNRecord.class), eq(7), eq(AccessOption.PERSISTENT)))
        .thenThrow(new ZkBadVersionException()).thenReturn(true);

    IdealState updatedIdealState = outcome.updateIdealState(helixManager, OFFLINE_TABLE_NAME);

    assertEquals(updatedIdealState.getRecord()
        .getSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY), "1000");
    verify(baseDataAccessor, times(2)).set(eq(idealStateKey.getPath()), any(ZNRecord.class), eq(7),
        eq(AccessOption.PERSISTENT));
  }

  @Test(dataProvider = "unusableResponseBodies")
  public void testUnreadableResponseIsRejected(String responseBody)
      throws Exception {
    startServer(SERVER_0, new TableSegmentValidationInfo(true, null, 1_000L));
    startServer(SERVER_1, 200, responseBody);
    wireHelix(serverToSegments(SERVER_0, Arrays.asList(SEGMENT_0), SERVER_1, Arrays.asList(SEGMENT_1)),
        Arrays.asList(SERVER_0, SERVER_1));

    ControllerApplicationException exception = expectFailure("Expected unreadable response to block");
    assertEquals(exception.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
  }

  @DataProvider(name = "nonRoutableServerStates")
  public Object[][] nonRoutableServerStates() {
    return new Object[][]{{"disabled"}, {"queriesDisabled"}, {"shuttingDown"}, {"missing"}};
  }

  @DataProvider(name = "unusableResponseBodies")
  public Object[][] unusableResponseBodies() {
    return new Object[][]{{"not-json"}, {"null"}};
  }

  private void startServer(String serverId, TableSegmentValidationInfo validationInfo)
      throws IOException {
    startServer(serverId, 200, JsonUtils.objectToString(validationInfo));
  }

  private void startServer(String serverId, int statusCode, String payload)
      throws IOException {
    FakeHttpServer server = new FakeHttpServer();
    HttpHandler handler = exchange -> {
      byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(statusCode, bytes.length);
      try (OutputStream stream = exchange.getResponseBody()) {
        stream.write(bytes);
      }
    };
    server.start(URI_PATH, handler);
    _servers.put(serverId, server);
  }

  private void wireHelix(Map<String, List<String>> serverToSegments, List<String> routableServers)
      throws InvalidConfigException {
    when(_helix.getServerToSegmentsMap(OFFLINE_TABLE_NAME)).thenReturn(serverToSegments);
    when(_helix.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView(serverToSegments));
    BiMap<String, String> endpoints = HashBiMap.create();
    int unreachablePort = 1;
    for (String server : serverToSegments.keySet()) {
      FakeHttpServer fakeServer = _servers.get(server);
      endpoints.put(server, fakeServer != null ? fakeServer._endpoint : "http://localhost:" + unreachablePort++);
      when(_helix.getHelixInstanceConfig(server)).thenReturn(
          routableServers.contains(server) ? enabledInstanceConfig(server) : disabledInstanceConfig(server));
    }
    when(_helix.getDataInstanceAdminEndpoints(anySet(), anyBoolean())).thenReturn(endpoints);
  }

  private void configureNonRoutableState(String server, String state)
      throws InvalidConfigException {
    if ("missing".equals(state)) {
      when(_helix.getHelixInstanceConfig(server)).thenReturn(null);
      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put(SERVER_0, _servers.get(SERVER_0)._endpoint);
      when(_helix.getDataInstanceAdminEndpoints(anySet(), anyBoolean())).thenReturn(endpoints);
      return;
    }

    InstanceConfig instanceConfig = enabledInstanceConfig(server);
    if ("disabled".equals(state)) {
      instanceConfig.setInstanceEnabled(false);
    } else if ("queriesDisabled".equals(state)) {
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.QUERIES_DISABLED, true);
    } else if ("shuttingDown".equals(state)) {
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
    } else {
      fail("Unknown state: " + state);
    }
    when(_helix.getHelixInstanceConfig(server)).thenReturn(instanceConfig);
  }

  private ControllerApplicationException expectFailure(String message) {
    try {
      long boundary = _validator.validate(OFFLINE_TABLE_NAME).getMaxEndTimeMs();
      fail(message + ", but returned boundary: " + boundary);
      throw new IllegalStateException("unreachable");
    } catch (ControllerApplicationException e) {
      return e;
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception type", e);
    }
  }

  private static InstanceConfig enabledInstanceConfig(String server) {
    InstanceConfig instanceConfig = new InstanceConfig(server);
    instanceConfig.setInstanceEnabled(true);
    return instanceConfig;
  }

  private static InstanceConfig disabledInstanceConfig(String server) {
    InstanceConfig instanceConfig = new InstanceConfig(server);
    instanceConfig.setInstanceEnabled(false);
    return instanceConfig;
  }

  private void setReplication(int replication) {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getReplication()).thenReturn(replication);
    when(_helix.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
  }

  private static Map<String, List<String>> serverToSegments(Object... serverThenSegments) {
    Map<String, List<String>> map = new HashMap<>();
    for (int i = 0; i < serverThenSegments.length; i += 2) {
      @SuppressWarnings("unchecked")
      List<String> segments = (List<String>) serverThenSegments[i + 1];
      map.put((String) serverThenSegments[i], segments);
    }
    return map;
  }

  private static ExternalView externalView(Map<String, List<String>> serverToSegments) {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      for (String segment : entry.getValue()) {
        externalView.setState(segment, entry.getKey(), SegmentStateModel.ONLINE);
      }
    }
    return externalView;
  }

  private static IdealState idealState(Map<String, List<String>> serverToSegments) {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      for (String segment : entry.getValue()) {
        idealState.setPartitionState(segment, entry.getKey(), SegmentStateModel.ONLINE);
      }
    }
    return idealState;
  }
}
