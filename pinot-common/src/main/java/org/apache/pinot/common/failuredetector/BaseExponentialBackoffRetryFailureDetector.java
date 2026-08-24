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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code BaseExponentialBackoffRetryFailureDetector} is a base failure detector implementation that retries the
 * unhealthy servers with exponential increasing delays.
 */
@ThreadSafe
public abstract class BaseExponentialBackoffRetryFailureDetector implements FailureDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExponentialBackoffRetryFailureDetector.class);

  // Number of consecutive queries for which a server fails to respond (no data table received) before the server is
  // proactively marked unhealthy. This detects a silent/half-dead server that is still TCP-connected (so no
  // channelInactive fires) without waiting for the eventual socket reset. The counter is reset on any successful
  // response from the server.
  public static final int CONSECUTIVE_FAILURES_THRESHOLD = 5;
  // Number of consecutive successful health probes required before an unhealthy server is re-admitted to routing.
  // Requiring sustained health (a "probation" phase) prevents flapping when a probe transiently succeeds against a
  // server that still cannot actually serve queries.
  public static final int HEALTHY_THRESHOLD = 3;

  protected final String _name = getClass().getSimpleName();
  protected final ConcurrentHashMap<String, RetryInfo> _unhealthyServerRetryInfoMap = new ConcurrentHashMap<>();
  protected final DelayQueue<RetryInfo> _retryInfoDelayQueue = new DelayQueue<>();
  // Tracks the number of consecutive queries for which a (currently healthy) server failed to respond. Used to
  // proactively mark a silent-but-connected server unhealthy without waiting for a transport-level event.
  protected final ConcurrentHashMap<String, AtomicInteger> _consecutiveNotRespondedMap = new ConcurrentHashMap<>();

  protected final List<Function<String, ServerState>> _unhealthyServerRetriers = new ArrayList<>();
  protected Consumer<String> _healthyServerNotifier;
  protected Consumer<String> _unhealthyServerNotifier;
  protected BrokerMetrics _brokerMetrics;
  protected long _retryInitialDelayNs;
  protected double _retryDelayFactor;
  protected double _retryDelayJitterFactor;
  protected int _maxRetries;
  // Threshold fields default to the hardcoded policy constants; @VisibleForTesting setters allow tests to vary them.
  protected volatile int _consecutiveFailuresThreshold = CONSECUTIVE_FAILURES_THRESHOLD;
  protected volatile int _healthyThreshold = HEALTHY_THRESHOLD;
  protected Thread _retryThread;

  protected volatile boolean _running;

  @Override
  public void init(PinotConfiguration config, BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
    long retryInitialDelayMs = config.getProperty(Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS,
        Broker.FailureDetector.DEFAULT_RETRY_INITIAL_DELAY_MS);
    _retryInitialDelayNs = TimeUnit.MILLISECONDS.toNanos(retryInitialDelayMs);
    _retryDelayFactor = config.getProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR,
        Broker.FailureDetector.DEFAULT_RETRY_DELAY_FACTOR);
    double jitterFactor = config.getProperty(Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_JITTER_FACTOR,
        Broker.FailureDetector.DEFAULT_RETRY_DELAY_JITTER_FACTOR);
    _retryDelayJitterFactor = Math.max(0.0, Math.min(1.0, jitterFactor));
    _maxRetries =
        config.getProperty(Broker.FailureDetector.CONFIG_OF_MAX_RETRIES, Broker.FailureDetector.DEFAULT_MAX_RETRIES);
    LOGGER.info("Initialized {} with retry initial delay: {}ms, exponential backoff factor: {}, jitter factor: {}, "
            + "max retries: {}, consecutive failures threshold: {}, healthy threshold: {} (probation re-probe reuses "
            + "the retry initial delay)", _name, retryInitialDelayMs, _retryDelayFactor, _retryDelayJitterFactor,
        _maxRetries, _consecutiveFailuresThreshold, _healthyThreshold);
  }

  @Override
  public void registerUnhealthyServerRetrier(Function<String, ServerState> unhealthyServerRetrier) {
    _unhealthyServerRetriers.add(unhealthyServerRetrier);
  }

  @Override
  public void registerHealthyServerNotifier(Consumer<String> healthyServerNotifier) {
    _healthyServerNotifier = healthyServerNotifier;
  }

  @Override
  public void registerUnhealthyServerNotifier(Consumer<String> unhealthyServerNotifier) {
    _unhealthyServerNotifier = unhealthyServerNotifier;
  }

  @Override
  public void start() {
    LOGGER.info("Starting {}", _name);
    _running = true;

    _retryThread = new Thread(() -> {
      while (_running) {
        try {
          RetryInfo retryInfo = _retryInfoDelayQueue.take();
          String instanceId = retryInfo._instanceId;
          if (_unhealthyServerRetryInfoMap.get(instanceId) != retryInfo) {
            LOGGER.info("Server: {} has been marked healthy, skipping the retry", instanceId);
            continue;
          }
          if (retryInfo._numRetries == _maxRetries) {
            LOGGER.warn("Unhealthy server: {} already reaches the max retries: {}, do not retry again and treat it "
                + "as healthy so that the listeners do not lose track of the server", instanceId, _maxRetries);
            markServerHealthy(instanceId);
            continue;
          }
          LOGGER.info("Retry unhealthy server: {}", instanceId);
          boolean recovered = true;
          for (Function<String, ServerState> unhealthyServerRetrier : _unhealthyServerRetriers) {
            ServerState serverState = unhealthyServerRetrier.apply(instanceId);
            if (serverState == ServerState.UNHEALTHY) {
              recovered = false;
              break;
            }
          }
          if (recovered) {
            // Passed a health probe. Require a configurable number of consecutive successful probes (a "probation"
            // phase) before re-admitting the server to routing. This prevents flapping when a probe transiently
            // succeeds against a server that still cannot actually serve queries.
            retryInfo._numConsecutiveSuccessfulProbes++;
            if (retryInfo._numConsecutiveSuccessfulProbes >= _healthyThreshold) {
              markServerHealthy(instanceId);
            } else {
              LOGGER.info("Server: {} passed health probe {}/{}, keeping it in probation before re-admitting",
                  instanceId, retryInfo._numConsecutiveSuccessfulProbes, _healthyThreshold);
              // Re-probe at a steady cadence (the retry initial delay) without growing the backoff, and keep the
              // server out of routing.
              retryInfo._retryTimeNs = System.nanoTime() + applyJitter(_retryInitialDelayNs);
              _retryInfoDelayQueue.offer(retryInfo);
            }
          } else {
            // Failed probe: reset probation progress and grow the backoff so a repeatedly-failing server is retried
            // less and less aggressively (instead of hammering it every retry interval).
            retryInfo._numConsecutiveSuccessfulProbes = 0;
            retryInfo._retryDelayNs = (long) (retryInfo._retryDelayNs * _retryDelayFactor);
            retryInfo._retryTimeNs = System.nanoTime() + applyJitter(retryInfo._retryDelayNs);
            retryInfo._numRetries++;
            _retryInfoDelayQueue.offer(retryInfo);
          }
        } catch (Exception e) {
          if (_running) {
            LOGGER.error("Caught exception in the retry thread, continuing with errors", e);
          }
        }
      }
    });
    _retryThread.setName("failure-detector-retry");
    _retryThread.setDaemon(true);
    _retryThread.start();
  }

  @Override
  public void markServerHealthy(String instanceId) {
    _consecutiveNotRespondedMap.remove(instanceId);
    _unhealthyServerRetryInfoMap.computeIfPresent(instanceId, (id, retryInfo) -> {
      LOGGER.info("Mark server: {} as healthy", instanceId);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size() - 1);
      _healthyServerNotifier.accept(instanceId);
      return null;
    });
  }

  @Override
  public void markServerUnhealthy(String instanceId) {
    _consecutiveNotRespondedMap.remove(instanceId);
    _unhealthyServerRetryInfoMap.computeIfAbsent(instanceId, id -> {
      LOGGER.warn("Mark server: {} as unhealthy", instanceId);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.UNHEALTHY_SERVERS, _unhealthyServerRetryInfoMap.size() + 1);
      _unhealthyServerNotifier.accept(instanceId);
      RetryInfo retryInfo = new RetryInfo(id);
      _retryInfoDelayQueue.offer(retryInfo);
      return retryInfo;
    });
  }

  @Override
  public void notifyServerResponded(String instanceId) {
    // A successful response clears any accumulated consecutive-failure count for this server.
    AtomicInteger counter = _consecutiveNotRespondedMap.get(instanceId);
    if (counter != null) {
      counter.set(0);
    }
  }

  @Override
  public void notifyServerNotResponded(String instanceId) {
    if (_consecutiveFailuresThreshold <= 0) {
      // Timeout-based detection disabled.
      return;
    }
    if (_unhealthyServerRetryInfoMap.containsKey(instanceId)) {
      // Already known unhealthy; the retry/probation loop owns its lifecycle now.
      return;
    }
    int count = _consecutiveNotRespondedMap.computeIfAbsent(instanceId, id -> new AtomicInteger()).incrementAndGet();
    if (count >= _consecutiveFailuresThreshold) {
      LOGGER.warn("Server: {} failed to respond to {} consecutive queries (threshold: {}), marking it unhealthy",
          instanceId, count, _consecutiveFailuresThreshold);
      markServerUnhealthy(instanceId);
    }
  }

  @Override
  public Set<String> getUnhealthyServers() {
    return _unhealthyServerRetryInfoMap.keySet();
  }

  @VisibleForTesting
  void setHealthyThreshold(int healthyThreshold) {
    _healthyThreshold = Math.max(1, healthyThreshold);
  }

  @VisibleForTesting
  void setConsecutiveFailuresThreshold(int consecutiveFailuresThreshold) {
    _consecutiveFailuresThreshold = consecutiveFailuresThreshold;
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}", _name);
    _running = false;

    try {
      _retryThread.interrupt();
      _retryThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for retry thread to finish", e);
    }
  }

  /**
   * Returns the given delay reduced by a random amount in [0, delay * jitterFactor], so the actual scheduled
   * delay falls in [delay * (1 - jitterFactor), delay]. This prevents synchronized retries when many brokers
   * detect the same server failure at the same time (thundering herd).
   */
  protected long applyJitter(long delayNs) {
    if (_retryDelayJitterFactor <= 0.0 || delayNs <= 0) {
      return delayNs;
    }
    long maxJitterNs = (long) (delayNs * _retryDelayJitterFactor);
    if (maxJitterNs <= 0) {
      return delayNs;
    }
    return delayNs - ThreadLocalRandom.current().nextLong(maxJitterNs + 1);
  }

  /**
   * Encapsulates the retry related information.
   */
  protected class RetryInfo implements Delayed {
    final String _instanceId;

    long _retryTimeNs;
    long _retryDelayNs;
    int _numRetries;
    // Number of consecutive successful health probes accumulated during the probation phase. Reset to 0 on any
    // failed probe. The server is only re-admitted once this reaches the configured healthy threshold.
    int _numConsecutiveSuccessfulProbes;

    RetryInfo(String instanceId) {
      _instanceId = instanceId;
      _retryTimeNs = System.nanoTime() + applyJitter(_retryInitialDelayNs);
      _retryDelayNs = _retryInitialDelayNs;
      _numRetries = 0;
      _numConsecutiveSuccessfulProbes = 0;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(_retryTimeNs - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      RetryInfo that = (RetryInfo) o;
      return Long.compare(_retryTimeNs, that._retryTimeNs);
    }
  }
}
