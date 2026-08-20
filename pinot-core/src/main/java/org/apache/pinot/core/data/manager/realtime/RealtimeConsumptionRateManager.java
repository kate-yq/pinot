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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for creating realtime consumption rate limiters.
 * It contains one rate limiter for the entire server and multiple table partition level rate limiters.
 * Server rate limiter is used to throttle the overall consumption rate of the server and configured via
 * cluster or server config.
 * For table partition level rate limiter, the rate limit value specified in StreamConfig of table config, is for the
 * entire topic. The effective rate limit for each partition is simply the specified rate limit divided by the
 * partition count.
 * This class leverages a cache for storing partition count for different topics as retrieving partition count from
 * stream is a bit expensive and also the same count will be used of all partition consumers of the same topic.
 */
public class RealtimeConsumptionRateManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumptionRateManager.class);
  private static final int CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES = 10;

  private static final String SERVER_CONSUMPTION_RATE_METRIC_KEY_NAME =
      ServerMeter.REALTIME_ROWS_CONSUMED.getMeterName();
  private static final String SERVER_BYTES_CONSUMPTION_RATE_METRIC_KEY_NAME =
      ServerMeter.REALTIME_BYTES_CONSUMED.getMeterName();
  private volatile ConsumptionRateLimiter _serverRateLimiter = NOOP_RATE_LIMITER;
  private volatile ConsumptionRateLimiter _serverBytesRateLimiter = NOOP_RATE_LIMITER;

  // stream config object is required for fetching the partition count from the stream
  private final LoadingCache<StreamConfig, Integer> _streamConfigToTopicPartitionCountMap;
  private volatile boolean _isThrottlingAllowed = false;

  @VisibleForTesting
  RealtimeConsumptionRateManager(LoadingCache<StreamConfig, Integer> streamConfigToTopicPartitionCountMap) {
    _streamConfigToTopicPartitionCountMap = streamConfigToTopicPartitionCountMap;
  }

  private static class InstanceHolder {
    private static final RealtimeConsumptionRateManager INSTANCE = new RealtimeConsumptionRateManager(
        buildCache(DEFAULT_PARTITION_COUNT_FETCHER, CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES, TimeUnit.MINUTES));
  }

  public static RealtimeConsumptionRateManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public void enableThrottling() {
    _isThrottlingAllowed = true;
  }

  public ConsumptionRateLimiter createServerRateLimiter(PinotConfiguration serverConfig, ServerMetrics serverMetrics) {
    double serverRateLimit =
        serverConfig.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
            CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT);
    if (serverRateLimit > 0) {
      LOGGER.info("Set up ConsumptionRateLimiter with rate limit: {}", serverRateLimit);
      MetricEmitter metricEmitter = new MetricEmitter(serverMetrics, SERVER_CONSUMPTION_RATE_METRIC_KEY_NAME,
          ServerGauge.CONSUMPTION_QUOTA_UTILIZATION);
      _serverRateLimiter = new RateLimiterImpl(serverRateLimit, metricEmitter, MessageBatch::getMessageCount);
    } else {
      LOGGER.info("ConsumptionRateLimiter is disabled");
      _serverRateLimiter = NOOP_RATE_LIMITER;
    }
    return _serverRateLimiter;
  }

  public ConsumptionRateLimiter getServerRateLimiter() {
    return _serverRateLimiter;
  }

  public ConsumptionRateLimiter createServerBytesRateLimiter(PinotConfiguration serverConfig,
      ServerMetrics serverMetrics) {
    double serverBytesRateLimit =
        serverConfig.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
            CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT_BYTES);
    if (serverBytesRateLimit > 0) {
      LOGGER.info("Set up bytes ConsumptionRateLimiter with rate limit: {} bytes/sec", serverBytesRateLimit);
      MetricEmitter metricEmitter =
          new MetricEmitter(serverMetrics, SERVER_BYTES_CONSUMPTION_RATE_METRIC_KEY_NAME,
              ServerGauge.BYTES_CONSUMPTION_QUOTA_UTILIZATION);
      _serverBytesRateLimiter =
          new RateLimiterImpl(serverBytesRateLimit, metricEmitter, MessageBatch::getBatchSizeBytes);
    } else {
      LOGGER.info("Bytes ConsumptionRateLimiter is disabled");
      _serverBytesRateLimiter = NOOP_RATE_LIMITER;
    }
    return _serverBytesRateLimiter;
  }

  public ConsumptionRateLimiter getServerBytesRateLimiter() {
    return _serverBytesRateLimiter;
  }

  public ConsumptionRateLimiter createBytesRateLimiter(StreamConfig streamConfig, String tableName,
      ServerMetrics serverMetrics, String metricKeyName) {
    if (streamConfig.getTopicConsumptionRateLimitBytes().isEmpty()) {
      return NOOP_RATE_LIMITER;
    }
    int partitionCount;
    try {
      partitionCount = _streamConfigToTopicPartitionCountMap.get(streamConfig);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    double topicBytesRateLimit = streamConfig.getTopicConsumptionRateLimitBytes().get();
    double partitionBytesRateLimit = topicBytesRateLimit / partitionCount;
    LOGGER.info("A bytes consumption rate limiter is set up for topic {} in table {} with rate limit: {} bytes/sec "
            + "(topic rate limit: {} bytes/sec, partition count: {})", streamConfig.getTopicName(), tableName,
        partitionBytesRateLimit, topicBytesRateLimit, partitionCount);
    MetricEmitter metricEmitter = new MetricEmitter(serverMetrics, metricKeyName,
        ServerGauge.BYTES_CONSUMPTION_QUOTA_UTILIZATION);
    return new RateLimiterImpl(partitionBytesRateLimit, metricEmitter, MessageBatch::getBatchSizeBytes);
  }

  @VisibleForTesting
  ConsumptionRateLimiter createBytesRateLimiter(StreamConfig streamConfig, String tableName) {
    return createBytesRateLimiter(streamConfig, tableName, null, null);
  }

  public ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, String tableName,
      ServerMetrics serverMetrics, String metricKeyName) {
    if (streamConfig.getTopicConsumptionRateLimitEvents().isEmpty()) {
      return NOOP_RATE_LIMITER;
    }
    int partitionCount;
    try {
      partitionCount = _streamConfigToTopicPartitionCountMap.get(streamConfig);
    } catch (ExecutionException e) {
      // Exception here means for some reason, partition count cannot be fetched from stream!
      throw new RuntimeException(e);
    }
    double topicRateLimit = streamConfig.getTopicConsumptionRateLimitEvents().get();
    double partitionRateLimit = topicRateLimit / partitionCount;
    LOGGER.info("A consumption rate limiter is set up for topic {} in table {} with rate limit: {} "
            + "(topic rate limit: {}, partition count: {})", streamConfig.getTopicName(), tableName, partitionRateLimit,
        topicRateLimit, partitionCount);
    MetricEmitter metricEmitter = new MetricEmitter(serverMetrics, metricKeyName,
        ServerGauge.CONSUMPTION_QUOTA_UTILIZATION);
    return new RateLimiterImpl(partitionRateLimit, metricEmitter, MessageBatch::getMessageCount);
  }

  @VisibleForTesting
  ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, String tableName) {
    return createRateLimiter(streamConfig, tableName, null, null);
  }

  @VisibleForTesting
  static LoadingCache<StreamConfig, Integer> buildCache(PartitionCountFetcher partitionCountFetcher,
      long duration, TimeUnit unit) {
    return CacheBuilder.newBuilder().refreshAfterWrite(duration, unit)
        .build(new CacheLoader<StreamConfig, Integer>() {
          @Override
          public Integer load(StreamConfig key)
              throws Exception {
            // this method is called the first time cache is used for the given streamConfig
            Integer count = partitionCountFetcher.fetch(key);
            // if the count cannot be fetched, don't throw exception; return 1.
            // The overall consumption rate will be higher, but we prefer that over not consuming at all.
            return count != null ? count : 1;
          }

          @Override
          public ListenableFuture<Integer> reload(StreamConfig key, Integer oldValue)
              throws Exception {
            // if partition count fetcher cannot fetch the value, old value is returned
            Integer count = partitionCountFetcher.fetch(key);
            return Futures.immediateFuture(count != null ? count : oldValue);
          }
        });
  }

  @FunctionalInterface
  public interface ConsumptionRateLimiter {
    void throttle(MessageBatch messageBatch);
  }

  @VisibleForTesting
  static final ConsumptionRateLimiter NOOP_RATE_LIMITER = b -> {
  };

  @VisibleForTesting
  static class RateLimiterImpl implements ConsumptionRateLimiter {
    private final double _rate;
    private final RateLimiter _rateLimiter;
    private final MetricEmitter _metricEmitter;
    private final ToLongFunction<MessageBatch> _amountExtractor;

    private RateLimiterImpl(double rate, MetricEmitter metricEmitter,
        ToLongFunction<MessageBatch> amountExtractor) {
      _rate = rate;
      _rateLimiter = RateLimiter.create(rate);
      _metricEmitter = metricEmitter;
      _amountExtractor = amountExtractor;
    }

    @Override
    public void throttle(MessageBatch messageBatch) {
      if (InstanceHolder.INSTANCE._isThrottlingAllowed) {
        long amount = _amountExtractor.applyAsLong(messageBatch);
        // Only emit metrics when throttling is allowed. Throttling is not enabled
        // until the server has passed startup checks. Otherwise, we will see
        // consumption well over 100% during startup.
        _metricEmitter.emitMetric(amount, _rate, Clock.systemUTC().instant());
        if (amount > 0) {
          // Guava RateLimiter.acquire() takes int permits; clamp to avoid overflow on large byte batches.
          int permits = amount > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) amount;
          _rateLimiter.acquire(permits);
        }
      }
    }

    @VisibleForTesting
    double getRate() {
      return _rate;
    }
  }

  @VisibleForTesting
  @FunctionalInterface
  interface PartitionCountFetcher {
    Integer fetch(StreamConfig streamConfig);
  }

  @VisibleForTesting
  static final PartitionCountFetcher DEFAULT_PARTITION_COUNT_FETCHER = streamConfig -> {
    String clientId =
        StreamConsumerFactory.getUniqueClientId(streamConfig.getTopicName() + "-consumption.rate.manager");
    StreamConsumerFactory factory = StreamConsumerFactoryProvider.create(streamConfig);
    try (StreamMetadataProvider streamMetadataProvider = factory.createStreamMetadataProvider(clientId)) {
      return streamMetadataProvider.fetchPartitionCount(/*maxWaitTimeMs*/10_000);
    } catch (Exception e) {
      LOGGER.warn("Error fetching metadata for topic {}", streamConfig.getTopicName(), e);
      return null;
    }
  };

  /**
   * Emits a gauge metric for the ratio of actual consumption rate to the rate limit.
   * Counts are aggregated over one minute; each minute the ratio percentage is calculated and emitted.
   */
  @VisibleForTesting
  static class MetricEmitter {

    private final ServerMetrics _serverMetrics;
    private final String _metricKeyName;
    private final ServerGauge _gauge;

    // state variables
    private long _previousMinute = -1;
    private long _aggregateCount = 0;

    public MetricEmitter(ServerMetrics serverMetrics, String metricKeyName, ServerGauge gauge) {
      _serverMetrics = serverMetrics;
      _metricKeyName = metricKeyName;
      _gauge = gauge;
    }

    // The server-wide rate limiter shares one MetricEmitter across all partition consumer threads, so this method
    // must be thread-safe. Method-level synchronization is the right granularity (rather than a finer lock or an
    // atomic counter): the minute rollover reads _aggregateCount, emits the gauge, then resets both _previousMinute
    // and _aggregateCount, so the accumulate and the rollover must be mutually exclusive as a single unit. Both
    // fields are private and touched only here (and getAggregateCount below), so this monitor covers every access;
    // any future accessor must take the same lock. Per-partition emitters are single-threaded and only pay an
    // uncontended lock per MessageBatch, which is negligible at batch granularity.
    synchronized int emitMetric(long count, double rateLimit, Instant now) {
      int ratioPercentage = 0;
      long nowInMinutes = now.getEpochSecond() / 60;
      if (nowInMinutes == _previousMinute) {
        _aggregateCount += count;
      } else {
        if (_previousMinute != -1) { // not first time
          double actualRate = _aggregateCount / ((nowInMinutes - _previousMinute) * 60.0);
          ratioPercentage = (int) Math.round(actualRate / rateLimit * 100);
          if (_serverMetrics != null) {
            _serverMetrics.setOrUpdateGauge(_metricKeyName, _gauge, ratioPercentage, ImmutableMap.of());
          }
        }
        _aggregateCount = count;
        _previousMinute = nowInMinutes;
      }
      return ratioPercentage;
    }

    @VisibleForTesting
    synchronized long getAggregateCount() {
      return _aggregateCount;
    }
  }
}
