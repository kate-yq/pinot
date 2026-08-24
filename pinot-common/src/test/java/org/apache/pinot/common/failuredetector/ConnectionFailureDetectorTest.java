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
package org.apache.pinot.common.failuredetector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ConnectionFailureDetectorTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";

  private BrokerMetrics _brokerMetrics;
  private FailureDetector _failureDetector;
  private UnhealthyServerRetrier _unhealthyServerRetrier;
  private HealthyServerNotifier _healthyServerNotifier;
  private UnhealthyServerNotifier _unhealthyServerNotifier;

  @BeforeMethod
  public void setUp() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, 100);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    _brokerMetrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _failureDetector = FailureDetectorFactory.getFailureDetector(config, _brokerMetrics);
    assertTrue(_failureDetector instanceof ConnectionFailureDetector);
    // These tests exercise the legacy "re-admit on first successful probe" behavior; probation is covered separately.
    ((BaseExponentialBackoffRetryFailureDetector) _failureDetector).setHealthyThreshold(1);
    _healthyServerNotifier = new HealthyServerNotifier();
    _failureDetector.registerHealthyServerNotifier(_healthyServerNotifier);
    _unhealthyServerNotifier = new UnhealthyServerNotifier();
    _failureDetector.registerUnhealthyServerNotifier(_unhealthyServerNotifier);
    _failureDetector.start();
  }

  @Test
  public void testConnectionFailure() {
    // No unhealthy servers initially
    verify(Collections.emptySet(), 0, 0);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Mark server unhealthy again should have no effect
    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Mark server healthy should remove it from the unhealthy servers and trigger a callback
    _failureDetector.markServerHealthy(INSTANCE_ID);
    verify(Collections.emptySet(), 1, 1);
  }

  @Test
  public void testRetryWithoutRecovery() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(10);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Should get 10 retries in 1s, then remove the failed server from the unhealthy servers.
    // Wait for up to 5s to avoid flakiness
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < Broker.FailureDetector.DEFAULT_MAX_RETRIES) {
        assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
        assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        return false;
      }
      assertEquals(numRetries, Broker.FailureDetector.DEFAULT_MAX_RETRIES);
      // There might be a small delay between the last retry and removing failed server from the unhealthy servers.
      // Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 10 retries");
  }

  @Test
  public void testRetryWithRecovery() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(6);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < 7) {
        // Avoid test flakiness by not making these assertions close to the end of the expected retry period
        if (numRetries > 0 && numRetries <= 5) {
          assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
          assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        }
        return false;
      }
      assertEquals(numRetries, 7);
      // There might be a small delay between the successful attempt and removing failed server from the unhealthy
      // servers. Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 7 retries");

    // Verify no further retries
    assertEquals(_unhealthyServerRetrier._retryUnhealthyServerCalled, 7);
  }

  @Test
  public void testRetryWithMultipleUnhealthyServerRetriers() {
    _unhealthyServerRetrier = new UnhealthyServerRetrier(5);
    _failureDetector.registerUnhealthyServerRetrier(_unhealthyServerRetrier);

    // This retrier will only be called after the first retrier starts returning HEALTHY. So we expect a total of 7
    // failures and 8 retries until the server is marked as healthy again.
    UnhealthyServerRetrier unhealthyServerRetrier2 = new UnhealthyServerRetrier(2);
    _failureDetector.registerUnhealthyServerRetrier(unhealthyServerRetrier2);

    // Register a retrier that isn't aware of the failing server. This should not affect the retry process.
    _failureDetector.registerUnhealthyServerRetrier(instanceId -> FailureDetector.ServerState.UNKNOWN);

    _failureDetector.markServerUnhealthy(INSTANCE_ID);
    verify(Collections.singleton(INSTANCE_ID), 1, 0);

    // Should retry until both unhealthy server retriers return that the server is healthy
    TestUtils.waitForCondition(aVoid -> {
      int numRetries = _unhealthyServerRetrier._retryUnhealthyServerCalled;
      if (numRetries < 8) {
        // Avoid test flakiness by not making these assertions close to the end of the expected retry period
        if (numRetries > 0 && numRetries <= 5) {
          assertEquals(_failureDetector.getUnhealthyServers(), Collections.singleton(INSTANCE_ID));
          assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS), 1);
        }
        return false;
      }
      assertEquals(numRetries, 8);
      // There might be a small delay between the successful attempt and removing failed server from the unhealthy
      // servers. Perform a check instead of an assertion.
      return _failureDetector.getUnhealthyServers().isEmpty()
          && MetricValueUtils.getGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS.getGaugeName()) == 0
          && _unhealthyServerNotifier._notifyUnhealthyServerCalled == 1
          && _healthyServerNotifier._notifyHealthyServerCalled == 1;
    }, 5_000L, "Failed to get 8 retries");

    // Verify no further retries
    assertEquals(_unhealthyServerRetrier._retryUnhealthyServerCalled, 8);
  }

  private void verify(Set<String> expectedUnhealthyServers, int expectedNotifyUnhealthyServerCalled,
      int expectedNotifyHealthyServerCalled) {
    assertEquals(_failureDetector.getUnhealthyServers(), expectedUnhealthyServers);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_brokerMetrics, BrokerGauge.UNHEALTHY_SERVERS),
        expectedUnhealthyServers.size());
    assertEquals(_unhealthyServerNotifier._notifyUnhealthyServerCalled, expectedNotifyUnhealthyServerCalled);
    assertEquals(_healthyServerNotifier._notifyHealthyServerCalled, expectedNotifyHealthyServerCalled);
  }

  /**
   * Builds an unstarted detector with the given jitter factor and initial delay.
   */
  private BaseExponentialBackoffRetryFailureDetector buildUnstartedDetector(double jitterFactor,
      long initialDelayMs) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, initialDelayMs);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_JITTER_FACTOR, jitterFactor);
    BrokerMetrics metrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    BaseExponentialBackoffRetryFailureDetector detector =
        (BaseExponentialBackoffRetryFailureDetector) FailureDetectorFactory.getFailureDetector(config, metrics);
    detector.registerUnhealthyServerNotifier(id -> { });
    detector.registerHealthyServerNotifier(id -> { });
    return detector;
  }

  @Test
  public void testApplyJitterWithZeroFactor() {
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.0, 100);
    for (int i = 0; i < 100; i++) {
      assertEquals(detector.applyJitter(1_000_000L), 1_000_000L);
    }
  }

  @Test
  public void testApplyJitterWithEqualJitter() {
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.5, 100);
    long base = 1_000_000L;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    Set<Long> distinctValues = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      long actual = detector.applyJitter(base);
      assertTrue(actual >= base / 2 && actual <= base, "actual=" + actual);
      min = Math.min(min, actual);
      max = Math.max(max, actual);
      distinctValues.add(actual);
    }
    // 1000 samples in a 500_000-wide range should yield many distinct values
    assertTrue(distinctValues.size() > 100,
        "Expected significant spread, got " + distinctValues.size() + " distinct values");
    // Distribution should cover most of the [base/2, base] range
    assertTrue(min < base * 0.6, "min=" + min + " did not approach base/2");
    assertTrue(max > base * 0.9, "max=" + max + " did not approach base");
  }

  @Test
  public void testApplyJitterWithFullJitter() {
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(1.0, 100);
    long base = 1_000_000L;
    long min = Long.MAX_VALUE;
    for (int i = 0; i < 1000; i++) {
      long actual = detector.applyJitter(base);
      assertTrue(actual >= 0 && actual <= base, "actual=" + actual);
      min = Math.min(min, actual);
    }
    // Full jitter should produce values close to 0 within 1000 samples
    assertTrue(min < base * 0.1, "min=" + min + " did not approach 0 with full jitter");
  }

  @Test
  public void testApplyJitterClampsFactorAboveOne() {
    // Factor > 1.0 should be clamped to 1.0 in init(), behaving as full jitter
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(5.0, 100);
    long base = 1_000_000L;
    for (int i = 0; i < 1000; i++) {
      long actual = detector.applyJitter(base);
      assertTrue(actual >= 0 && actual <= base, "actual=" + actual + " out of [0, base]");
    }
  }

  @Test
  public void testApplyJitterEdgeCases() {
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.5, 100);
    // Zero delay -> zero
    assertEquals(detector.applyJitter(0L), 0L);
    // Negative delay -> unchanged (short-circuited by delayNs <= 0 guard)
    assertEquals(detector.applyJitter(-100L), -100L);
    // Delay of 1ns with factor 0.5 -> maxJitter floors to 0 -> short-circuited, returns delay unchanged
    assertEquals(detector.applyJitter(1L), 1L);
  }

  @Test
  public void testThunderingHerdSpreadWithJitter() {
    // Long initial delay so the (unstarted) queue accumulates everything before any retry could fire
    long initialDelayMs = 60_000L;
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.5, initialDelayMs);
    int numServers = 200;
    for (int i = 0; i < numServers; i++) {
      detector.markServerUnhealthy("Server_" + i);
    }
    assertEquals(detector._retryInfoDelayQueue.size(), numServers);

    long minTimeNs = Long.MAX_VALUE;
    long maxTimeNs = Long.MIN_VALUE;
    Set<Long> distinctTimes = new HashSet<>();
    for (BaseExponentialBackoffRetryFailureDetector.RetryInfo info : detector._retryInfoDelayQueue) {
      minTimeNs = Math.min(minTimeNs, info._retryTimeNs);
      maxTimeNs = Math.max(maxTimeNs, info._retryTimeNs);
      distinctTimes.add(info._retryTimeNs);
    }

    // With factor 0.5 over a 60s base delay, jitter window is ~30s. Expect a real spread, not bunching.
    long spreadMs = TimeUnit.NANOSECONDS.toMillis(maxTimeNs - minTimeNs);
    assertTrue(spreadMs > 5_000L,
        "Expected jittered retry times to span >5s, got " + spreadMs + "ms");
    // Most retry times should be unique (the random draws collide rarely at nanosecond resolution)
    assertTrue(distinctTimes.size() > numServers * 0.9,
        "Expected most retry times unique, got " + distinctTimes.size() + "/" + numServers);
  }

  @Test
  public void testThunderingHerdNoJitterIsBunched() {
    // Confirms the baseline: without jitter, simultaneous failures all schedule into a tiny window,
    // which is exactly the thundering-herd pathology that jitter fixes.
    long initialDelayMs = 60_000L;
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.0, initialDelayMs);
    int numServers = 200;
    for (int i = 0; i < numServers; i++) {
      detector.markServerUnhealthy("Server_" + i);
    }
    assertEquals(detector._retryInfoDelayQueue.size(), numServers);

    long minTimeNs = Long.MAX_VALUE;
    long maxTimeNs = Long.MIN_VALUE;
    for (BaseExponentialBackoffRetryFailureDetector.RetryInfo info : detector._retryInfoDelayQueue) {
      minTimeNs = Math.min(minTimeNs, info._retryTimeNs);
      maxTimeNs = Math.max(maxTimeNs, info._retryTimeNs);
    }

    // Without jitter, the only spread is System.nanoTime() drift across the loop body — well under 1s.
    long spreadMs = TimeUnit.NANOSECONDS.toMillis(maxTimeNs - minTimeNs);
    assertTrue(spreadMs < 1_000L,
        "Without jitter, expected schedules bunched within 1s, got " + spreadMs + "ms");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fix #1: proactively mark a silent-but-connected server unhealthy after N consecutive no-response queries.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testConsecutiveFailuresTripUnhealthy() {
    // Default consecutive failures threshold is 5.
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.0, 100);
    String instanceId = "Server_localhost_2000";

    // First (threshold - 1) failures should not trip.
    for (int i = 0; i < BaseExponentialBackoffRetryFailureDetector.CONSECUTIVE_FAILURES_THRESHOLD - 1; i++) {
      detector.notifyServerNotResponded(instanceId);
      assertTrue(detector.getUnhealthyServers().isEmpty());
    }
    // The threshold-th consecutive failure trips it.
    detector.notifyServerNotResponded(instanceId);
    assertEquals(detector.getUnhealthyServers(), Collections.singleton(instanceId));
  }

  @Test
  public void testSuccessResetsConsecutiveFailures() {
    BaseExponentialBackoffRetryFailureDetector detector = buildUnstartedDetector(0.0, 100);
    String instanceId = "Server_localhost_2001";

    for (int i = 0; i < BaseExponentialBackoffRetryFailureDetector.CONSECUTIVE_FAILURES_THRESHOLD - 1; i++) {
      detector.notifyServerNotResponded(instanceId);
    }
    // A successful response resets the counter.
    detector.notifyServerResponded(instanceId);
    // Another (threshold - 1) failures should still not trip because the counter was reset.
    for (int i = 0; i < BaseExponentialBackoffRetryFailureDetector.CONSECUTIVE_FAILURES_THRESHOLD - 1; i++) {
      detector.notifyServerNotResponded(instanceId);
    }
    assertTrue(detector.getUnhealthyServers().isEmpty());
  }

  @Test
  public void testConsecutiveFailuresDetectionDisabled() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    BrokerMetrics metrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    BaseExponentialBackoffRetryFailureDetector detector =
        (BaseExponentialBackoffRetryFailureDetector) FailureDetectorFactory.getFailureDetector(config, metrics);
    detector.setConsecutiveFailuresThreshold(0);
    detector.registerUnhealthyServerNotifier(id -> { });
    detector.registerHealthyServerNotifier(id -> { });
    String instanceId = "Server_localhost_2002";
    for (int i = 0; i < 100; i++) {
      detector.notifyServerNotResponded(instanceId);
    }
    assertTrue(detector.getUnhealthyServers().isEmpty());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fix #4: probation - require N consecutive successful probes before re-admitting, do not flap on a single success.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testProbationRequiresConsecutiveHealthyProbes() {
    int healthyThreshold = 3;
    HealthyServerNotifier healthyNotifier = new HealthyServerNotifier();
    UnhealthyServerNotifier unhealthyNotifier = new UnhealthyServerNotifier();
    // Retrier that always reports the server as healthy.
    UnhealthyServerRetrier retrier = new UnhealthyServerRetrier(0);
    BaseExponentialBackoffRetryFailureDetector detector = buildStartedDetector(healthyThreshold, 100,
        healthyNotifier, unhealthyNotifier, retrier);
    try {
      detector.markServerUnhealthy(INSTANCE_ID);
      // Server should only be re-admitted (healthy notifier fired) after exactly healthyThreshold successful probes.
      TestUtils.waitForCondition(aVoid -> healthyNotifier._notifyHealthyServerCalled == 1, 5_000L,
          "Server was not marked healthy after sustained successful probes");
      assertEquals(retrier._retryUnhealthyServerCalled, healthyThreshold,
          "A single successful probe must not re-admit; exactly healthyThreshold probes are required");
      assertTrue(detector.getUnhealthyServers().isEmpty());
    } finally {
      detector.stop();
    }
  }

  @Test
  public void testProbationResetsOnFailedProbe() {
    int healthyThreshold = 3;
    HealthyServerNotifier healthyNotifier = new HealthyServerNotifier();
    UnhealthyServerNotifier unhealthyNotifier = new UnhealthyServerNotifier();
    // Scripted probe results: H, H, U (resets probation), then H, H, H (recovers). 6 probes total.
    ScriptedRetrier retrier = new ScriptedRetrier(FailureDetector.ServerState.HEALTHY,
        FailureDetector.ServerState.HEALTHY, FailureDetector.ServerState.UNHEALTHY, FailureDetector.ServerState.HEALTHY,
        FailureDetector.ServerState.HEALTHY, FailureDetector.ServerState.HEALTHY);
    BaseExponentialBackoffRetryFailureDetector detector = buildStartedDetector(healthyThreshold, 100,
        healthyNotifier, unhealthyNotifier, retrier);
    try {
      detector.markServerUnhealthy(INSTANCE_ID);
      TestUtils.waitForCondition(aVoid -> healthyNotifier._notifyHealthyServerCalled == 1, 5_000L,
          "Server was not marked healthy after the final 3 consecutive successful probes");
      // The interrupting failed probe reset the count, so recovery only happened after all 6 scripted probes.
      assertEquals(retrier._retryUnhealthyServerCalled, 6);
      assertEquals(healthyNotifier._notifyHealthyServerCalled, 1);
    } finally {
      detector.stop();
    }
  }

  private BaseExponentialBackoffRetryFailureDetector buildStartedDetector(int healthyThreshold, long retryDelayMs,
      Consumer<String> healthyNotifier, Consumer<String> unhealthyNotifier,
      Function<String, FailureDetector.ServerState> retrier) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.Type.CONNECTION.name());
    // The retry initial delay doubles as the probation re-probe cadence.
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, retryDelayMs);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    config.setProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_JITTER_FACTOR, 0.0);
    BrokerMetrics metrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    BaseExponentialBackoffRetryFailureDetector detector =
        (BaseExponentialBackoffRetryFailureDetector) FailureDetectorFactory.getFailureDetector(config, metrics);
    detector.setHealthyThreshold(healthyThreshold);
    detector.registerHealthyServerNotifier(healthyNotifier);
    detector.registerUnhealthyServerNotifier(unhealthyNotifier);
    detector.registerUnhealthyServerRetrier(retrier);
    detector.start();
    return detector;
  }

  private static class ScriptedRetrier implements Function<String, FailureDetector.ServerState> {
    int _retryUnhealthyServerCalled = 0;
    final FailureDetector.ServerState[] _states;

    ScriptedRetrier(FailureDetector.ServerState... states) {
      _states = states;
    }

    @Override
    public FailureDetector.ServerState apply(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      FailureDetector.ServerState state =
          _states[Math.min(_retryUnhealthyServerCalled, _states.length - 1)];
      _retryUnhealthyServerCalled++;
      return state;
    }
  }

  @AfterClass
  public void tearDown() {
    _failureDetector.stop();
  }

  private static class HealthyServerNotifier implements Consumer<String> {
    int _notifyHealthyServerCalled = 0;

    @Override
    public void accept(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyHealthyServerCalled++;
    }
  }

  private static class UnhealthyServerNotifier implements Consumer<String> {
    int _notifyUnhealthyServerCalled = 0;

    @Override
    public void accept(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _notifyUnhealthyServerCalled++;
    }
  }

  private static class UnhealthyServerRetrier implements Function<String, FailureDetector.ServerState> {
    int _retryUnhealthyServerCalled = 0;
    final int _numFailures;

    UnhealthyServerRetrier(int numFailures) {
      _numFailures = numFailures;
    }

    @Override
    public FailureDetector.ServerState apply(String instanceId) {
      assertEquals(instanceId, INSTANCE_ID);
      _retryUnhealthyServerCalled++;
      return _retryUnhealthyServerCalled > _numFailures ? FailureDetector.ServerState.HEALTHY
          : FailureDetector.ServerState.UNHEALTHY;
    }
  }
}
