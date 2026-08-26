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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.ServerQueryInfoFetcher;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Validates that the OFFLINE segments of a hybrid table are ready before its explicit time boundary is advanced.
 */
class HybridTimeBoundaryValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridTimeBoundaryValidator.class);
  private static final int MAX_UNVALIDATED_SEGMENTS_TO_LOG = 10;

  private final PinotHelixResourceManager _resourceManager;
  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;

  HybridTimeBoundaryValidator(PinotHelixResourceManager resourceManager, Executor executor,
      HttpClientConnectionManager connectionManager) {
    _resourceManager = resourceManager;
    _executor = executor;
    _connectionManager = connectionManager;
  }

  /**
   * Validates serving coverage and returns the maximum end time reported by eligible servers.
   */
  ValidationOutcome validate(String offlineTableName)
      throws InvalidConfigException {
    Map<String, List<String>> serverToSegments = _resourceManager.getServerToSegmentsMap(offlineTableName);
    ValidationResult validationResult = fetchValidationResults(offlineTableName, serverToSegments.keySet());
    ExternalView externalView = _resourceManager.getTableExternalView(offlineTableName);
    if (externalView == null) {
      throw new ControllerApplicationException(LOGGER,
          "Could not validate table segment status for table: " + offlineTableName + ". ExternalView is missing.",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    Set<String> ignoredServers = getIgnoredServers(serverToSegments, externalView);
    rejectPotentialOptionalReplicas(offlineTableName, serverToSegments, externalView, ignoredServers);
    rejectInvalidResponses(offlineTableName, validationResult, ignoredServers);
    Set<String> validatedServers = new HashSet<>(validationResult._validServerEndTimes.keySet());
    validatedServers.removeAll(ignoredServers);
    rejectNonResponders(offlineTableName, serverToSegments.keySet(), validationResult, ignoredServers);
    TableConfig tableConfig = _resourceManager.getTableConfig(offlineTableName);
    int requiredServingReplicas = Math.min(2, tableConfig.getReplication());
    requireServingCoverage(offlineTableName, serverToSegments, externalView, validatedServers, ignoredServers,
        requiredServingReplicas);

    if (!ignoredServers.isEmpty()) {
      LOGGER.info("Ignored servers that brokers cannot route to while validating segment status for table: {}. "
          + "servers={}", offlineTableName, ignoredServers);
    }
    return new ValidationOutcome(getMaxEndTimeMs(validatedServers, validationResult), serverToSegments);
  }

  private ValidationResult fetchValidationResults(String offlineTableName, Set<String> servers)
      throws InvalidConfigException {
    BiMap<String, String> serverEndpoints = _resourceManager.getDataInstanceAdminEndpoints(servers, true);
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndpoints);
    Map<String, String> urlToServer = new HashMap<>();
    List<String> serverUrls = new ArrayList<>();
    for (Map.Entry<String, String> entry : serverEndpoints.entrySet()) {
      String segmentStatusEndpoint = entry.getValue() + "/tables/" + offlineTableName + "/allSegmentsLoaded";
      urlToServer.put(segmentStatusEndpoint, entry.getKey());
      serverUrls.add(segmentStatusEndpoint);
    }
    CompletionServiceHelper.CompletionServiceResponse response =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10_000);
    return parseResponses(offlineTableName, urlToServer, response);
  }

  /**
   * Returns servers that cannot serve any assigned replica.
   */
  private Set<String> getIgnoredServers(Map<String, List<String>> serverToSegments, ExternalView externalView) {
    Set<String> ignoredServers = serversWithOnlyErrorSegments(serverToSegments, externalView);
    ServerQueryInfoFetcher serverQueryInfoFetcher = new ServerQueryInfoFetcher(_resourceManager);
    for (String server : serverToSegments.keySet()) {
      if (!isRoutableByBrokers(serverQueryInfoFetcher, server)) {
        ignoredServers.add(server);
      }
    }
    return ignoredServers;
  }

  /**
   * Rejects segments that have a potential optional replica on a routable server.
   */
  private static void rejectPotentialOptionalReplicas(String offlineTableName,
      Map<String, List<String>> serverToSegments, ExternalView externalView, Set<String> ignoredServers) {
    Set<String> segmentsWithOptionalReplicas =
        getSegmentsWithPotentialOptionalReplicas(serverToSegments, externalView, ignoredServers);
    if (!segmentsWithOptionalReplicas.isEmpty()) {
      List<String> sample = sortedSample(segmentsWithOptionalReplicas);
      throw new ControllerApplicationException(LOGGER,
          "Could not validate table segment status for table: " + offlineTableName + ". "
              + segmentsWithOptionalReplicas.size() + " segment(s) have routable replicas that are missing or "
              + "OFFLINE in ExternalView and may still be treated as new, e.g. " + sample,
          Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private static void rejectInvalidResponses(String offlineTableName, ValidationResult validationResult,
      Set<String> ignoredServers) {
    List<String> blockingInvalidReasons = new ArrayList<>();
    for (Map.Entry<String, String> entry : validationResult._invalidServerReasons.entrySet()) {
      if (!ignoredServers.contains(entry.getKey())) {
        blockingInvalidReasons.add(entry.getKey() + ": " + entry.getValue());
      }
    }
    throwIfInvalid(offlineTableName, blockingInvalidReasons);
  }

  private static void rejectNonResponders(String offlineTableName, Set<String> assignedServers,
      ValidationResult validationResult, Set<String> ignoredServers) {
    Set<String> nonResponders = new HashSet<>(assignedServers);
    nonResponders.removeAll(validationResult._validServerEndTimes.keySet());
    nonResponders.removeAll(validationResult._invalidServerReasons.keySet());
    nonResponders.removeAll(ignoredServers);
    if (!nonResponders.isEmpty()) {
      List<String> sortedNonResponders = new ArrayList<>(nonResponders);
      Collections.sort(sortedNonResponders);
      throw new ControllerApplicationException(LOGGER,
          "Could not validate table segment status for table: " + offlineTableName + ". Servers did not respond and "
              + "are still routable by brokers: " + sortedNonResponders, Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  /**
   * Requires every assigned segment to have the configured number of validated ONLINE or CONSUMING replicas.
   */
  private static void requireServingCoverage(String offlineTableName,
      Map<String, List<String>> serverToSegments, ExternalView externalView, Set<String> validatedServers,
      Set<String> ignoredServers, int requiredServingReplicas) {
    Map<String, Integer> servingReplicaCounts = new HashMap<>();
    for (String server : validatedServers) {
      for (String segment : serverToSegments.getOrDefault(server, Collections.emptyList())) {
        Map<String, String> stateMap = externalView.getStateMap(segment);
        if (stateMap != null && isServingState(stateMap.get(server))) {
          servingReplicaCounts.merge(segment, 1, Integer::sum);
        }
      }
    }
    Set<String> underReplicatedSegments = new HashSet<>();
    for (List<String> segments : serverToSegments.values()) {
      for (String segment : segments) {
        if (servingReplicaCounts.getOrDefault(segment, 0) < requiredServingReplicas) {
          underReplicatedSegments.add(segment);
        }
      }
    }
    if (!underReplicatedSegments.isEmpty()) {
      throw new ControllerApplicationException(LOGGER,
          "Could not validate table segment status for table: " + offlineTableName + ". "
              + underReplicatedSegments.size() + " segment(s) have fewer than " + requiredServingReplicas
              + " validated serving replicas, e.g. " + sortedSample(underReplicatedSegments)
              + ". Servers ignored as not serving: " + ignoredServers, Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private static long getMaxEndTimeMs(Set<String> validatedServers, ValidationResult validationResult) {
    long maxEndTimeMs = -1;
    for (String server : validatedServers) {
      maxEndTimeMs = Math.max(maxEndTimeMs, validationResult._validServerEndTimes.get(server));
    }
    return maxEndTimeMs;
  }

  private static List<String> sortedSample(Set<String> values) {
    List<String> sample = new ArrayList<>(values);
    Collections.sort(sample);
    return sample.subList(0, Math.min(sample.size(), MAX_UNVALIDATED_SEGMENTS_TO_LOG));
  }

  private static ValidationResult parseResponses(String offlineTableName, Map<String, String> urlToServer,
      CompletionServiceHelper.CompletionServiceResponse serviceResponse) {
    Map<String, Long> validServerEndTimes = new HashMap<>();
    Map<String, String> invalidServerReasons = new HashMap<>();

    for (Map.Entry<String, String> entry : serviceResponse._httpResponses.entrySet()) {
      String server = resolveServerFromResponseKey(entry.getKey(), urlToServer);
      String serverLabel = server != null ? server : entry.getKey();
      TableSegmentValidationInfo validationInfo;
      try {
        validationInfo = JsonUtils.stringToObject(entry.getValue(), TableSegmentValidationInfo.class);
      } catch (JsonProcessingException e) {
        LOGGER.warn("Could not parse segment validation response from server: {} for table: {}", serverLabel,
            offlineTableName, e);
        continue;
      }
      if (validationInfo == null) {
        LOGGER.warn("Segment validation response from server: {} for table: {} is null", serverLabel,
            offlineTableName);
        continue;
      }
      if (!validationInfo.isValid()) {
        invalidServerReasons.put(serverLabel, validationInfo.getInvalidReason());
        continue;
      }
      if (server != null) {
        validServerEndTimes.put(server, validationInfo.getMaxEndTimeMs());
      }
    }
    return new ValidationResult(validServerEndTimes, invalidServerReasons);
  }

  private static void throwIfInvalid(String offlineTableName, List<String> invalidServerReasons) {
    if (!invalidServerReasons.isEmpty()) {
      Collections.sort(invalidServerReasons);
      throw new ControllerApplicationException(LOGGER,
          "Table segment validation failed for table: " + offlineTableName + ". errors=["
              + String.join("; ", invalidServerReasons) + "]",
          Response.Status.PRECONDITION_FAILED);
    }
  }

  /**
   * Returns servers whose assigned replicas are all ERROR in ExternalView.
   */
  private static Set<String> serversWithOnlyErrorSegments(Map<String, List<String>> serverToSegments,
      ExternalView externalView) {
    Set<String> errorOnlyServers = new HashSet<>();
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      boolean allError = !entry.getValue().isEmpty();
      for (String segment : entry.getValue()) {
        Map<String, String> stateMap = externalView.getStateMap(segment);
        if (stateMap == null || !SegmentStateModel.ERROR.equals(stateMap.get(entry.getKey()))) {
          allError = false;
          break;
        }
      }
      if (allError) {
        errorOnlyServers.add(entry.getKey());
      }
    }
    return errorOnlyServers;
  }

  /**
   * Returns segments with a routable replica missing or OFFLINE in ExternalView and no ERROR replica.
   */
  private static Set<String> getSegmentsWithPotentialOptionalReplicas(
      Map<String, List<String>> serverToSegments,
      ExternalView externalView, Set<String> ignoredServers) {
    Map<String, Set<String>> segmentToServers = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      for (String segment : entry.getValue()) {
        segmentToServers.computeIfAbsent(segment, key -> new HashSet<>()).add(entry.getKey());
      }
    }

    Set<String> potentiallyNewSegments = new HashSet<>();
    for (Map.Entry<String, Set<String>> entry : segmentToServers.entrySet()) {
      Map<String, String> stateMap = externalView.getStateMap(entry.getKey());
      boolean hasErrorReplica = false;
      for (String server : entry.getValue()) {
        if (stateMap != null && SegmentStateModel.ERROR.equals(stateMap.get(server))) {
          hasErrorReplica = true;
          break;
        }
      }
      if (hasErrorReplica) {
        // ERROR retires the broker's new-segment special case, so missing replicas are no longer optional candidates.
        continue;
      }
      for (String server : entry.getValue()) {
        String state = stateMap != null ? stateMap.get(server) : null;
        if (!ignoredServers.contains(server) && !isServingState(state)) {
          potentiallyNewSegments.add(entry.getKey());
          break;
        }
      }
    }
    return potentiallyNewSegments;
  }

  private static boolean isServingState(@Nullable String state) {
    return SegmentStateModel.ONLINE.equals(state) || SegmentStateModel.CONSUMING.equals(state);
  }

  private static boolean isSameAssignment(Map<String, List<String>> first, Map<String, List<String>> second) {
    if (!first.keySet().equals(second.keySet())) {
      return false;
    }
    for (Map.Entry<String, List<String>> entry : first.entrySet()) {
      List<String> otherSegments = second.get(entry.getKey());
      if (otherSegments == null || !new HashSet<>(entry.getValue()).equals(new HashSet<>(otherSegments))) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, List<String>> getServerToSegmentsMap(IdealState idealState) {
    Map<String, List<String>> serverToSegments = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> segmentEntry : idealState.getRecord().getMapFields().entrySet()) {
      for (Map.Entry<String, String> stateEntry : segmentEntry.getValue().entrySet()) {
        if (!SegmentStateModel.OFFLINE.equals(stateEntry.getValue())) {
          serverToSegments.computeIfAbsent(stateEntry.getKey(), key -> new ArrayList<>()).add(segmentEntry.getKey());
        }
      }
    }
    return serverToSegments;
  }

  private static boolean isRoutableByBrokers(ServerQueryInfoFetcher serverQueryInfoFetcher, String server) {
    ServerQueryInfoFetcher.ServerQueryInfo serverQueryInfo = serverQueryInfoFetcher.getServerQueryInfo(server);
    return serverQueryInfo != null && serverQueryInfo.isHelixEnabled() && !serverQueryInfo.isQueriesDisabled()
        && !serverQueryInfo.isShutdownInProgress();
  }

  /**
   * Resolves the server from a response key with an optional request-count suffix.
   */
  @Nullable
  @VisibleForTesting
  static String resolveServerFromResponseKey(String responseKey, Map<String, String> urlToServer) {
    String server = urlToServer.get(responseKey);
    if (server != null) {
      return server;
    }
    int suffixIndex = responseKey.lastIndexOf("__");
    if (suffixIndex > 0 && StringUtils.isNumeric(responseKey.substring(suffixIndex + 2))) {
      return urlToServer.get(responseKey.substring(0, suffixIndex));
    }
    return null;
  }

  private static class ValidationResult {
    final Map<String, Long> _validServerEndTimes;
    final Map<String, String> _invalidServerReasons;

    ValidationResult(Map<String, Long> validServerEndTimes, Map<String, String> invalidServerReasons) {
      _validServerEndTimes = validServerEndTimes;
      _invalidServerReasons = invalidServerReasons;
    }
  }

  static class ValidationOutcome {
    private final long _maxEndTimeMs;
    private final Map<String, List<String>> _serverToSegments;

    ValidationOutcome(long maxEndTimeMs, Map<String, List<String>> serverToSegments) {
      _maxEndTimeMs = maxEndTimeMs;
      _serverToSegments = serverToSegments;
    }

    long getMaxEndTimeMs() {
      return _maxEndTimeMs;
    }

    /**
     * Writes the boundary only when the current IdealState still has the validated assignment.
     * Uses direct CAS because {@link HelixHelper#updateIdealState} can apply later assignment updaters in the same
     * batch.
     */
    IdealState updateIdealState(HelixManager helixManager, String tableNameWithType)
        throws Exception {
      AtomicBoolean assignmentChanged = new AtomicBoolean();
      AtomicReference<IdealState> updatedIdealState = new AtomicReference<>();
      RetryPolicies.exponentialBackoffRetryPolicy(5, 1_000L, 1.2f).attempt(() -> {
        HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
        PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(tableNameWithType);
        IdealState currentIdealState = dataAccessor.getProperty(idealStateKey);
        if (!matchesAssignment(currentIdealState)) {
          assignmentChanged.set(true);
          return true;
        }

        IdealState newIdealState = HelixHelper.cloneIdealState(currentIdealState);
        newIdealState.getRecord()
            .setSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY, Long.toString(_maxEndTimeMs));
        try {
          if (dataAccessor.getBaseDataAccessor()
              .set(idealStateKey.getPath(), newIdealState.getRecord(), currentIdealState.getRecord().getVersion(),
                  AccessOption.PERSISTENT)) {
            updatedIdealState.set(newIdealState);
            return true;
          }
        } catch (ZkBadVersionException e) {
          return false;
        }
        return false;
      });
      if (assignmentChanged.get()) {
        throw new ControllerApplicationException(LOGGER,
            "Segment assignment changed while validating table: " + tableNameWithType + ". Retry the request.",
            Response.Status.SERVICE_UNAVAILABLE);
      }
      return updatedIdealState.get();
    }

    boolean matchesAssignment(IdealState idealState) {
      return isSameAssignment(_serverToSegments, getServerToSegmentsMap(idealState));
    }
  }
}
