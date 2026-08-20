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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.LoadingCache;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.ConsumptionRateLimiter;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.MetricEmitter;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.PartitionCountFetcher;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.RateLimiterImpl;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.NOOP_RATE_LIMITER;
import static org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.buildCache;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class RealtimeConsumptionRateManagerTest {
  private static final int NUM_PARTITIONS_TOPIC_A = 10;
  private static final int NUM_PARTITIONS_TOPIC_B = 20;
  private static final Double RATE_LIMIT_FOR_ENTIRE_TOPIC = 50.0;
  private static final Double BYTES_RATE_LIMIT_FOR_ENTIRE_TOPIC = 1_000_000.0; // 1 MB/sec per topic
  private static final String TABLE_NAME = "table-XYZ";
  private static final double DELTA = 0.0001;
  private static final StreamConfig STREAM_CONFIG_A = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_B = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_C = mock(StreamConfig.class);
  private static final PinotConfiguration SERVER_CONFIG_1 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_2 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_3 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_4 = mock(PinotConfiguration.class);
  private static RealtimeConsumptionRateManager _consumptionRateManager;

  static {
    LoadingCache<StreamConfig, Integer> cache = mock(LoadingCache.class);
    try {
      when(cache.get(STREAM_CONFIG_A)).thenReturn(NUM_PARTITIONS_TOPIC_A);
      when(cache.get(STREAM_CONFIG_B)).thenReturn(NUM_PARTITIONS_TOPIC_B);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    when(STREAM_CONFIG_A.getTopicConsumptionRateLimitEvents()).thenReturn(Optional.of(RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_B.getTopicConsumptionRateLimitEvents()).thenReturn(Optional.of(RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_C.getTopicConsumptionRateLimitEvents()).thenReturn(Optional.empty());

    when(STREAM_CONFIG_A.getTopicConsumptionRateLimitBytes())
        .thenReturn(Optional.of(BYTES_RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_B.getTopicConsumptionRateLimitBytes())
        .thenReturn(Optional.of(BYTES_RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_C.getTopicConsumptionRateLimitBytes()).thenReturn(Optional.empty());

    _consumptionRateManager = new RealtimeConsumptionRateManager(cache);

    when(SERVER_CONFIG_1.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(5.0);
    when(SERVER_CONFIG_2.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(2.5);
    when(SERVER_CONFIG_3.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(0.0);
    when(SERVER_CONFIG_4.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(-1.0);

    when(SERVER_CONFIG_1.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT_BYTES)).thenReturn(500_000.0);
    when(SERVER_CONFIG_2.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT_BYTES)).thenReturn(250_000.0);
    when(SERVER_CONFIG_3.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT_BYTES)).thenReturn(0.0);
    when(SERVER_CONFIG_4.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT_BYTES)).thenReturn(-1.0);
  }

  @Test
  public void testCreateRateLimiter() {
    // topic A
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME);
    assertEquals(5.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic B
    rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_B, TABLE_NAME);
    assertEquals(2.5, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic C
    rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

  @Test
  public void testCreateServerRateLimiter() {
    // Server config 1
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_1, null);
    assertEquals(5.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // Server config 2
    rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_2, null);
    assertEquals(2.5, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // Server config 3
    rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_3, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    // Server config 4
    rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_4, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

  @Test
  public void testBuildCache()
      throws Exception {
    PartitionCountFetcher partitionCountFetcher = mock(PartitionCountFetcher.class);
    LoadingCache<StreamConfig, Integer> cache = buildCache(partitionCountFetcher, 500, TimeUnit.MILLISECONDS);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_A)).thenReturn(10);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_B)).thenReturn(20);
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // call fetcher in load method
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 20); // call fetcher in load method
    assertEquals((int) cache.get(STREAM_CONFIG_B), 20); // use cache
    verify(partitionCountFetcher, times(1)).fetch(STREAM_CONFIG_A); // count changes
    verify(partitionCountFetcher, times(1)).fetch(STREAM_CONFIG_B); // count changes
    when(partitionCountFetcher.fetch(STREAM_CONFIG_A)).thenReturn(11);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_B)).thenReturn(21);
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 20); // use cache
    Thread.sleep(550); // wait till cache expires
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 21); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_B), 21); // use cache
    verify(partitionCountFetcher, times(2)).fetch(STREAM_CONFIG_A);
    verify(partitionCountFetcher, times(2)).fetch(STREAM_CONFIG_B);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_A)).thenReturn(null); // unsuccessful fetch
    when(partitionCountFetcher.fetch(STREAM_CONFIG_B)).thenReturn(22);
    Thread.sleep(550); // wait till cache expires
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 22); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_B), 22); // use cache
    verify(partitionCountFetcher, times(3)).fetch(STREAM_CONFIG_A);
    verify(partitionCountFetcher, times(3)).fetch(STREAM_CONFIG_B);

    // unsuccessful fetch in the first call for config C
    when(partitionCountFetcher.fetch(STREAM_CONFIG_C)).thenReturn(null); // unsuccessful fetch
    assertEquals((int) cache.get(STREAM_CONFIG_C), 1); // call fetcher in load method
    assertEquals((int) cache.get(STREAM_CONFIG_C), 1); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_C), 1); // use cache
    verify(partitionCountFetcher, times(1)).fetch(STREAM_CONFIG_C);
  }

  @Test
  public void testMetricEmitter() {

    // setup metric emitter
    double rateLimit = 2; // unit: msgs/sec
    double rateLimitInMinutes = rateLimit * 60;
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MetricEmitter metricEmitter = new MetricEmitter(serverMetrics, "tableA-topicB-partition5",
        ServerGauge.CONSUMPTION_QUOTA_UTILIZATION);

    // 1st minute: no metrics should be emitted in the first minute
    int[] numMsgs = {10, 20, 5, 25};
    Instant now = Clock.fixed(Instant.parse("2022-08-10T12:00:02Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numMsgs[0], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:10Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numMsgs[1], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:30Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numMsgs[2], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:55Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numMsgs[3], rateLimit, now), 0);

    // 2nd minute: metric should be emitted
    now = Clock.fixed(Instant.parse("2022-08-10T12:01:05Z"), ZoneOffset.UTC).instant();
    int sumOfMsgsInPrevMinute = sum(numMsgs);
    int expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{35};
    assertEquals(metricEmitter.emitMetric(numMsgs[0], rateLimit, now), expectedRatio);

    // 3rd minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:02:25Z"), ZoneOffset.UTC).instant();
    sumOfMsgsInPrevMinute = sum(numMsgs);
    expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{0};
    assertEquals(metricEmitter.emitMetric(numMsgs[0], rateLimit, now), expectedRatio);

    // 4th minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:03:15Z"), ZoneOffset.UTC).instant();
    sumOfMsgsInPrevMinute = sum(numMsgs);
    expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{10, 20};
    assertEquals(metricEmitter.emitMetric(numMsgs[0], rateLimit, now), expectedRatio);
    now = Clock.fixed(Instant.parse("2022-08-10T12:03:20Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numMsgs[1], rateLimit, now), expectedRatio);

    // 5th minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:04:30Z"), ZoneOffset.UTC).instant();
    sumOfMsgsInPrevMinute = sum(numMsgs);
    expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{5};
    assertEquals(metricEmitter.emitMetric(numMsgs[0], rateLimit, now), expectedRatio);
  }

  @Test
  public void testCreateBytesRateLimiter() {
    // topic A: 1_000_000 bytes/sec / 10 partitions = 100_000 bytes/sec per partition
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createBytesRateLimiter(STREAM_CONFIG_A, TABLE_NAME);
    assertEquals(100_000.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic B: 1_000_000 bytes/sec / 20 partitions = 50_000 bytes/sec per partition
    rateLimiter = _consumptionRateManager.createBytesRateLimiter(STREAM_CONFIG_B, TABLE_NAME);
    assertEquals(50_000.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic C: no bytes rate limit configured
    rateLimiter = _consumptionRateManager.createBytesRateLimiter(STREAM_CONFIG_C, TABLE_NAME);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

  @Test
  public void testCreateServerBytesRateLimiter() {
    // Server config 1: 500_000 bytes/sec
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createServerBytesRateLimiter(SERVER_CONFIG_1, null);
    assertEquals(500_000.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // Server config 2: 250_000 bytes/sec
    rateLimiter = _consumptionRateManager.createServerBytesRateLimiter(SERVER_CONFIG_2, null);
    assertEquals(250_000.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // Server config 3: 0.0 → disabled
    rateLimiter = _consumptionRateManager.createServerBytesRateLimiter(SERVER_CONFIG_3, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    // Server config 4: -1.0 → disabled
    rateLimiter = _consumptionRateManager.createServerBytesRateLimiter(SERVER_CONFIG_4, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

  @Test
  public void testBytesMetricEmitter() {

    double rateLimit = 1000; // unit: bytes/sec
    double rateLimitInMinutes = rateLimit * 60;
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MetricEmitter metricEmitter = new MetricEmitter(serverMetrics, "tableA-topicB-partition5",
        ServerGauge.BYTES_CONSUMPTION_QUOTA_UTILIZATION);

    // 1st minute: no metrics should be emitted in the first minute
    long[] numBytes = {1024, 2048, 512, 4096};
    Instant now = Clock.fixed(Instant.parse("2022-08-10T12:00:02Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numBytes[0], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:10Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numBytes[1], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:30Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numBytes[2], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:55Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numBytes[3], rateLimit, now), 0);

    // 2nd minute: metric should be emitted
    now = Clock.fixed(Instant.parse("2022-08-10T12:01:05Z"), ZoneOffset.UTC).instant();
    long sumOfBytesInPrevMinute = sumLongs(numBytes);
    int expectedRatio = calcExpectedBytesRatio(rateLimitInMinutes, sumOfBytesInPrevMinute);
    numBytes = new long[]{3000};
    assertEquals(metricEmitter.emitMetric(numBytes[0], rateLimit, now), expectedRatio);

    // 3rd minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:02:25Z"), ZoneOffset.UTC).instant();
    sumOfBytesInPrevMinute = sumLongs(numBytes);
    expectedRatio = calcExpectedBytesRatio(rateLimitInMinutes, sumOfBytesInPrevMinute);
    numBytes = new long[]{0};
    assertEquals(metricEmitter.emitMetric(numBytes[0], rateLimit, now), expectedRatio);

    // 4th minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:03:15Z"), ZoneOffset.UTC).instant();
    sumOfBytesInPrevMinute = sumLongs(numBytes);
    expectedRatio = calcExpectedBytesRatio(rateLimitInMinutes, sumOfBytesInPrevMinute);
    numBytes = new long[]{5000, 10000};
    assertEquals(metricEmitter.emitMetric(numBytes[0], rateLimit, now), expectedRatio);
    now = Clock.fixed(Instant.parse("2022-08-10T12:03:20Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(numBytes[1], rateLimit, now), expectedRatio);

    // 5th minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:04:30Z"), ZoneOffset.UTC).instant();
    sumOfBytesInPrevMinute = sumLongs(numBytes);
    expectedRatio = calcExpectedBytesRatio(rateLimitInMinutes, sumOfBytesInPrevMinute);
    numBytes = new long[]{512};
    assertEquals(metricEmitter.emitMetric(numBytes[0], rateLimit, now), expectedRatio);
  }

  @Test
  public void testBytesMetricEmitterWithNullServerMetrics() {
    // BytesMetricEmitter should tolerate null serverMetrics (used in test/partition-level overload)
    MetricEmitter metricEmitter = new MetricEmitter(null, "test-key",
        ServerGauge.BYTES_CONSUMPTION_QUOTA_UTILIZATION);

    Instant minute1 = Clock.fixed(Instant.parse("2022-08-10T12:00:05Z"), ZoneOffset.UTC).instant();
    assertEquals(metricEmitter.emitMetric(2048, 1000.0, minute1), 0);

    Instant minute2 = Clock.fixed(Instant.parse("2022-08-10T12:01:10Z"), ZoneOffset.UTC).instant();
    int ratio = metricEmitter.emitMetric(1024, 1000.0, minute2);
    // 2048 bytes in 60 seconds = 34.13 bytes/sec → 34.13 / 1000 * 100 ≈ 3%
    assertEquals(ratio, (int) Math.round(2048.0 / 60.0 / 1000.0 * 100));
  }

  /**
   * The server-wide rate limiter shares a single {@link MetricEmitter} across all partition consumer threads.
   * This verifies that concurrent per-minute aggregation does not lose counts. Without thread-safety, the
   * unsynchronized read-modify-write of the aggregate loses updates and the emitted utilization is under-reported.
   */
  @Test
  public void testMetricEmitterIsThreadSafeUnderConcurrentAggregation()
      throws Exception {
    int numThreads = 16;
    int iterationsPerThread = 100_000;
    long expectedTotal = (long) numThreads * iterationsPerThread;

    MetricEmitter metricEmitter =
        new MetricEmitter(null, "thread-safety-test", ServerGauge.CONSUMPTION_QUOTA_UTILIZATION);

    // All increments target the same minute so every call takes the accumulate path (_aggregateCount += count).
    // The rate limit value is irrelevant here; the test reads the aggregated count directly via getAggregateCount().
    Instant sameMinute = Clock.fixed(Instant.parse("2022-08-10T12:00:30Z"), ZoneOffset.UTC).instant();
    double rateLimit = 1000.0;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch startLatch = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(numThreads);
      for (int t = 0; t < numThreads; t++) {
        futures.add(executor.submit((Callable<Void>) () -> {
          startLatch.await();
          for (int i = 0; i < iterationsPerThread; i++) {
            metricEmitter.emitMetric(1, rateLimit, sameMinute);
          }
          return null;
        }));
      }
      // Release all threads at once to maximize contention.
      startLatch.countDown();
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }

    // Assert the aggregated count directly. Without synchronization the concurrent read-modify-write of
    // _aggregateCount loses updates and the observed count is smaller than the expected total.
    long aggregatedCount = metricEmitter.getAggregateCount();
    assertEquals(aggregatedCount, expectedTotal,
        "Concurrent emitMetric lost updates: expected " + expectedTotal + " but aggregated " + aggregatedCount);
  }

  private int calcExpectedRatio(double rateLimitInMinutes, int sumOfMsgsInPrevMinute) {
    return (int) Math.round(sumOfMsgsInPrevMinute / rateLimitInMinutes * 100);
  }

  private int calcExpectedBytesRatio(double rateLimitInMinutes, long sumOfBytesInPrevMinute) {
    return (int) Math.round(sumOfBytesInPrevMinute / rateLimitInMinutes * 100);
  }

  private int sum(int[] numMsgs) {
    return Arrays.stream(numMsgs).sum();
  }

  private long sumLongs(long[] values) {
    return Arrays.stream(values).sum();
  }
}
