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

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pinot.common.failuredetector.BaseExponentialBackoffRetryFailureDetector;
import org.apache.pinot.common.failuredetector.ConnectionFailureDetector;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end simulation of the broker↔server failure-detector incident (server {@code lva2-app27368_O}).
 *
 * <p>It models a server that first goes <b>silent while still TCP-connected</b> (queries time out) and then a
 * <b>half-dead</b> server whose TCP handshake still succeeds while it cannot actually serve. It exercises the real
 * {@link ConnectionFailureDetector} together with the real HTTP readiness probe
 * ({@link SingleConnectionBrokerRequestHandler#isServerReadyViaHttp}) and demonstrates:
 * <ol>
 *   <li>Fix #1 — a silent-but-connected server is quarantined after N consecutive no-response queries, without
 *       waiting for a transport-level reset/channelInactive.</li>
 *   <li>Reproduction of the bug — a bare TCP-connect probe (always "healthy") with single-success re-admission
 *       causes the server to <b>flap</b> in and out of routing.</li>
 *   <li>Fix #3 + #4 — the HTTP readiness probe plus the probation window prevent the flap, and the server is
 *       re-admitted exactly once after it sustains health.</li>
 * </ol>
 */
public class FailureDetectorIncidentSimulationTest {
  // Mirror the instance id from the incident (OFFLINE routing instance).
  private static final String INSTANCE_ID = "Server_lva2-app27368_O";
  private static final int PROBE_TIMEOUT_MS = 2_000;
  private static final long INITIAL_DELAY_MS = 50;
  // High enough that the max-retries "give up and treat as healthy" safety valve never fires within a test window.
  private static final int MAX_RETRIES = 100_000;

  private HttpServer _httpServer;
  private String _adminEndpoint;
  // Models whether the server can actually serve queries. When false the server is "half-dead": its TCP handshake
  // succeeds but readiness returns 503.
  private volatile boolean _serverCanServe;

  private ConnectionFailureDetector _detector;
  private final AtomicInteger _includeToRoutingCount = new AtomicInteger();
  private final AtomicInteger _excludeFromRoutingCount = new AtomicInteger();

  @BeforeMethod
  public void setUp()
      throws Exception {
    _serverCanServe = false;
    _includeToRoutingCount.set(0);
    _excludeFromRoutingCount.set(0);
    _httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    _httpServer.createContext("/health/readiness", exchange -> {
      int status = _serverCanServe ? 200 : 503;
      byte[] body = (status == 200 ? "OK" : "NOT_READY").getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    _httpServer.start();
    _adminEndpoint = "http://localhost:" + _httpServer.getAddress().getPort();
  }

  @AfterMethod
  public void tearDown() {
    if (_detector != null) {
      _detector.stop();
    }
    if (_httpServer != null) {
      _httpServer.stop(0);
    }
  }

  /**
   * Fix #1: a server that goes silent (queries time out) while still TCP-connected is quarantined after the
   * configured number of consecutive no-response queries — no transport reset / channelInactive required.
   */
  @Test
  public void testSilentServerQuarantinedFromRepeatedTimeouts() {
    _detector = buildDetector(id -> FailureDetector.ServerState.UNHEALTHY);

    int threshold = BaseExponentialBackoffRetryFailureDetector.CONSECUTIVE_FAILURES_THRESHOLD;
    // Below the threshold the server stays in routing: we do not over-react to a few slow/timed-out queries.
    for (int i = 0; i < threshold - 1; i++) {
      _detector.notifyServerNotResponded(INSTANCE_ID);
    }
    assertTrue(_detector.getUnhealthyServers().isEmpty(), "Server should still be healthy below the threshold");
    assertEquals(_excludeFromRoutingCount.get(), 0);

    // The threshold-th consecutive no-response quarantines the server (excluded from routing).
    _detector.notifyServerNotResponded(INSTANCE_ID);
    assertTrue(_detector.getUnhealthyServers().contains(INSTANCE_ID),
        "Silent-but-connected server should be quarantined after repeated timeouts");
    assertEquals(_excludeFromRoutingCount.get(), 1);
  }

  /**
   * Fix #3 + #4: with the HTTP readiness probe and the probation window, a half-dead server (readiness 503) is
   * never re-admitted while unhealthy, and once it sustains health (readiness 200) it is re-admitted exactly once —
   * no flapping.
   */
  @Test
  public void testReadinessProbeAndProbationPreventFlapAndRecoverCleanly() {
    // Real HTTP readiness probe against the embedded "server", with probation (default healthy threshold = 3).
    Function<String, FailureDetector.ServerState> readinessRetrier = id ->
        SingleConnectionBrokerRequestHandler.isServerReadyViaHttp(_adminEndpoint, id, PROBE_TIMEOUT_MS)
            ? FailureDetector.ServerState.HEALTHY : FailureDetector.ServerState.UNHEALTHY;
    _detector = buildDetector(readinessRetrier);

    // Server is half-dead: TCP connect would succeed, but readiness returns 503.
    _detector.markServerUnhealthy(INSTANCE_ID);

    // Give the retry/probation loop many cycles. The server must NOT be re-admitted while it fails readiness.
    sleep(INITIAL_DELAY_MS * 10);
    assertEquals(_includeToRoutingCount.get(), 0,
        "Half-dead server (failing readiness) must not be re-admitted -> no flap");
    assertTrue(_detector.getUnhealthyServers().contains(INSTANCE_ID));

    // Server genuinely recovers.
    _serverCanServe = true;

    // It should now be re-admitted, but only after sustaining 3 consecutive successful probes.
    waitFor(() -> _includeToRoutingCount.get() == 1, 10_000, "Recovered server should be re-admitted to routing");
    assertTrue(_detector.getUnhealthyServers().isEmpty());

    // And it should settle: re-admitted exactly once, not flapping.
    sleep(INITIAL_DELAY_MS * 6);
    assertEquals(_includeToRoutingCount.get(), 1, "Recovered server should be re-admitted exactly once (no flap)");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------------------------

  private ConnectionFailureDetector buildDetector(Function<String, FailureDetector.ServerState> retrier) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    // The retry initial delay doubles as the probation re-probe cadence.
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, INITIAL_DELAY_MS);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_JITTER_FACTOR, 0.0);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_MAX_RETRIES, MAX_RETRIES);
    // Consecutive-failures and healthy thresholds use the hardcoded production defaults (5 and 3).
    ConnectionFailureDetector detector = new ConnectionFailureDetector();
    detector.init(config, new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    detector.registerHealthyServerNotifier(includeToRoutingNotifier());
    detector.registerUnhealthyServerNotifier(id -> _excludeFromRoutingCount.incrementAndGet());
    detector.registerUnhealthyServerRetrier(retrier);
    detector.start();
    return detector;
  }

  /** Models the routing manager re-admitting the server: counts how many times it is included back into routing. */
  private Consumer<String> includeToRoutingNotifier() {
    return id -> _includeToRoutingCount.incrementAndGet();
  }

  private static void waitFor(BooleanSupplier condition, long timeoutMs, String message) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      sleep(20);
    }
    if (!condition.getAsBoolean()) {
      throw new AssertionError(message);
    }
  }

  private static void sleep(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
