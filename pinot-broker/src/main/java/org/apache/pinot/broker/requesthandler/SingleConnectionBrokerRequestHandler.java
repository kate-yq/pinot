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
package org.apache.pinot.broker.requesthandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SingleConnectionBrokerRequestHandler</code> class is a thread-safe broker request handler using a single
 * connection per server to route the queries.
 */
@ThreadSafe
public class SingleConnectionBrokerRequestHandler extends BaseSingleStageBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleConnectionBrokerRequestHandler.class);

  private final BrokerReduceService _brokerReduceService;
  private final QueryRouter _queryRouter;
  private final FailureDetector _failureDetector;
  // Connect + read timeout for the HTTP readiness health probe used by the unhealthy-server recovery check.
  private static final int HEALTH_PROBE_TIMEOUT_MS = 2000;

  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      NettyConfig nettyConfig, TlsConfig tlsConfig, ServerRoutingStatsManager serverRoutingStatsManager,
      FailureDetector failureDetector, ThreadAccountant threadAccountant) {
    super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        threadAccountant);
    _brokerReduceService = new BrokerReduceService(_config);
    int sendRequestMaxAttempts = config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_SEND_REQUEST_MAX_ATTEMPTS,
        CommonConstants.Broker.DEFAULT_BROKER_SEND_REQUEST_MAX_ATTEMPTS);
    _queryRouter = new QueryRouter(_brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant,
        sendRequestMaxAttempts);
    _failureDetector = failureDetector;
    _failureDetector.registerUnhealthyServerRetrier(this::retryUnhealthyServer);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void shutDown() {
    super.shutDown();
    _queryRouter.shutDown();
    _brokerReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    BrokerRequest offlineBrokerRequest = route.getOfflineBrokerRequest();
    BrokerRequest realtimeBrokerRequest = route.getRealtimeBrokerRequest();

    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;
    if (requestContext.isSampledRequest()) {
      serverBrokerRequest.getPinotQuery().putToQueryOptions(CommonConstants.Broker.Request.TRACE, "true");
    }

    String rawTableName = TableNameBuilder.extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, rawTableName, route, timeoutMs);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    if (asyncQueryResponse.getStatus() == QueryResponse.Status.TIMED_OUT) {
      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS : BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }
    if (asyncQueryResponse.getFailedServer() != null) {
      _failureDetector.markServerUnhealthy(asyncQueryResponse.getFailedServer().getInstanceId());
    }
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER,
        System.nanoTime() - scatterGatherStartTimeNs);
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getServerStats());

    int numServersQueried = finalResponses.size();
    long totalResponseSize = 0;
    Map<ServerRoutingInstance, DataTable> dataTableMap = Maps.newHashMapWithExpectedSize(numServersQueried);
    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : finalResponses.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      String instanceId = entry.getKey().getInstanceId();
      if (dataTable != null) {
        dataTableMap.put(entry.getKey(), dataTable);
        totalResponseSize += serverResponse.getResponseSize();
        // Server responded: clear any accumulated consecutive-failure count.
        _failureDetector.notifyServerResponded(instanceId);
      } else {
        serversNotResponded.add(entry.getKey());
        // Server failed to respond (e.g. query timed out while the server was still TCP-connected). Feed this into
        // the failure detector so a silent-but-connected server can be quarantined after repeated failures, without
        // waiting for a transport-level channelInactive event.
        _failureDetector.notifyServerNotResponded(instanceId);
      }
    }
    int numServersResponded = dataTableMap.size();

    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeoutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, serverBrokerRequest, dataTableMap,
            reduceTimeoutMs, _brokerMetrics);
    long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(reduceTimeNanos));

    Exception brokerRequestSendException = asyncQueryResponse.getException();
    if (brokerRequestSendException != null) {
      brokerResponse.addException(
          new QueryProcessingException(QueryErrorCode.BROKER_REQUEST_SEND, brokerRequestSendException.getMessage()));
    }
    int numServersNotResponded = serversNotResponded.size();
    if (numServersNotResponded != 0) {
      brokerResponse.addException(new QueryProcessingException(QueryErrorCode.SERVER_NOT_RESPONDING,
          String.format("%d servers %s not responded", numServersNotResponded, serversNotResponded)));

      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED
          : BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE, totalResponseSize);

    return brokerResponse;
  }

  /**
   * Check if a server that was previously detected as unhealthy is now healthy.
   */
  public FailureDetector.ServerState retryUnhealthyServer(String instanceId) {
    LOGGER.info("Retrying unhealthy server: {}", instanceId);
    ServerInstance serverInstance = _routingManager.getEnabledServerInstanceMap().get(instanceId);

    if (serverInstance == null) {
      LOGGER.info("Failed to find enabled server: {} in routing manager, skipping the retry", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }

    // Could occur if the cluster is only serving multi-stage queries
    if (!_queryRouter.hasChannel(serverInstance)) {
      return FailureDetector.ServerState.UNKNOWN;
    }

    // First gate: a transport connection must be establishable.
    if (!_queryRouter.connect(serverInstance)) {
      LOGGER.warn("Still cannot connect to server: {}, retry later", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }

    // Second gate: a bare TCP connect can succeed against a half-dead server whose kernel still completes the
    // handshake while the query engine cannot serve. Require the server to report readiness via its admin endpoint so
    // we don't prematurely re-admit (and flap) such a server.
    if (!isServerReady(serverInstance)) {
      return FailureDetector.ServerState.UNHEALTHY;
    }

    LOGGER.info("Successfully probed server: {}, marking it healthy", instanceId);
    return FailureDetector.ServerState.HEALTHY;
  }

  /**
   * Application-level health probe: issues an HTTP GET to the server admin {@code /health/readiness} endpoint and
   * treats a 200 response as ready. Returns {@code true} (does not block recovery) when the admin endpoint is
   * unknown, since in that case we fall back to the transport-level connect check only.
   */
  private boolean isServerReady(ServerInstance serverInstance) {
    String adminEndpoint = serverInstance.getAdminEndpoint();
    if (StringUtils.isBlank(adminEndpoint)) {
      LOGGER.debug("No admin endpoint for server: {}, skipping HTTP readiness probe", serverInstance.getInstanceId());
      return true;
    }
    return isServerReadyViaHttp(adminEndpoint, serverInstance.getInstanceId(), HEALTH_PROBE_TIMEOUT_MS);
  }

  @VisibleForTesting
  static boolean isServerReadyViaHttp(String adminEndpoint, String instanceId, int timeoutMs) {
    String url = adminEndpoint + "/health/readiness";
    try {
      SimpleHttpResponse response =
          HttpClient.getInstance().sendRequest(ClassicRequestBuilder.get(new URI(url)).build(), timeoutMs);
      int statusCode = response.getStatusCode();
      if (statusCode == 200) {
        return true;
      }
      LOGGER.warn("Readiness probe for server: {} at {} returned status: {}", instanceId, url, statusCode);
      return false;
    } catch (Exception e) {
      LOGGER.warn("Readiness probe failed for server: {} at {}: {}", instanceId, url, e.getMessage());
      return false;
    }
  }
}
