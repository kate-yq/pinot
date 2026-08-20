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
package org.apache.pinot.spi.accounting;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ThreadResourceUsageProvider} class providing the functionality of measuring the CPU time
 * and allocateBytes (JVM heap) for the current thread.
 */
public class ThreadResourceUsageProvider {
  private ThreadResourceUsageProvider() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadResourceUsageProvider.class);

  // used for getting the memory allocation function in hotspot jvm
  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();
  // HotSpot extension of ThreadMXBean, null when running on a JVM that does not provide it. Resolved once here
  // because getCurrentThreadAllocatedBytes() is on the query sampling hot path and must not go through reflection.
  @Nullable
  private static final com.sun.management.ThreadMXBean SUN_MX_BEAN;
  private static final boolean IS_CURRENT_THREAD_CPU_TIME_SUPPORTED = MX_BEAN.isCurrentThreadCpuTimeSupported();
  private static final boolean IS_THREAD_ALLOCATED_MEMORY_SUPPORTED;
  private static final boolean IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT;
  private static boolean _isThreadCpuTimeMeasurementEnabled = false;
  private static boolean _isThreadMemoryMeasurementEnabled = false;

  //initialize the com.sun.management.ThreadMXBean related variables
  static {
    com.sun.management.ThreadMXBean sunThreadMXBean = null;
    if (MX_BEAN instanceof com.sun.management.ThreadMXBean) {
      sunThreadMXBean = (com.sun.management.ThreadMXBean) MX_BEAN;
    } else {
      LOGGER.error("Not able to load com.sun.management.ThreadMXBean, you are probably not using Hotspot jvm");
    }
    SUN_MX_BEAN = sunThreadMXBean;
    IS_THREAD_ALLOCATED_MEMORY_SUPPORTED =
        sunThreadMXBean != null && sunThreadMXBean.isThreadAllocatedMemorySupported();
    IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT =
        IS_THREAD_ALLOCATED_MEMORY_SUPPORTED && sunThreadMXBean.isThreadAllocatedMemoryEnabled();
  }

  public static int getThreadCount() {
    return MX_BEAN.getThreadCount();
  }

  public static long getTotalStartedThreadCount() {
    return MX_BEAN.getTotalStartedThreadCount();
  }

  public static long getCurrentThreadCpuTime() {
    return _isThreadCpuTimeMeasurementEnabled ? MX_BEAN.getCurrentThreadCpuTime() : 0;
  }

  public static long getCurrentThreadAllocatedBytes() {
    // _isThreadMemoryMeasurementEnabled implies IS_THREAD_ALLOCATED_MEMORY_SUPPORTED, which implies SUN_MX_BEAN != null
    return _isThreadMemoryMeasurementEnabled ? SUN_MX_BEAN.getThreadAllocatedBytes(Thread.currentThread().getId()) : 0;
  }

  /// Returns an approximation of the total garbage collection time in milliseconds.
  public static long getGcTime() {
    long totalGCTime = 0;
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      long gcTime = gcBean.getCollectionTime();
      if (gcTime > 0) {
        totalGCTime += gcTime;
      }
    }
    return totalGCTime;
  }

  public static boolean isThreadCpuTimeMeasurementEnabled() {
    return _isThreadCpuTimeMeasurementEnabled;
  }

  public static void setThreadCpuTimeMeasurementEnabled(boolean enable) {
    _isThreadCpuTimeMeasurementEnabled = enable && IS_CURRENT_THREAD_CPU_TIME_SUPPORTED;
  }

  public static boolean isThreadMemoryMeasurementEnabled() {
    return _isThreadMemoryMeasurementEnabled;
  }

  public static void setThreadMemoryMeasurementEnabled(boolean enable) {
    boolean isThreadAllocateMemoryEnabled = false;
    if (IS_THREAD_ALLOCATED_MEMORY_SUPPORTED) {
      try {
        if (SUN_MX_BEAN.isThreadAllocatedMemoryEnabled() != enable) {
          SUN_MX_BEAN.setThreadAllocatedMemoryEnabled(enable);
        }
        isThreadAllocateMemoryEnabled = SUN_MX_BEAN.isThreadAllocatedMemoryEnabled();
      } catch (UnsupportedOperationException e) {
        LOGGER.error("Not able to call isThreadAllocatedMemoryEnabled or setThreadAllocatedMemoryEnabled, ", e);
      }
    }
    _isThreadMemoryMeasurementEnabled = enable && isThreadAllocateMemoryEnabled;
  }

  static {
    LOGGER.info("Current thread cpu time measurement supported: {}", IS_CURRENT_THREAD_CPU_TIME_SUPPORTED);
    LOGGER.info("Current thread allocated bytes measurement supported: {}", IS_THREAD_ALLOCATED_MEMORY_SUPPORTED);
    LOGGER.info("Current thread allocated bytes measurement enabled default: {}",
        IS_THREAD_ALLOCATED_MEMORY_ENABLED_DEFAULT);
  }
}
