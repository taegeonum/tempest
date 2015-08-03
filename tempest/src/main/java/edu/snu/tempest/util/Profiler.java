/*
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
package edu.snu.tempest.util;

import com.sun.management.OperatingSystemMXBean;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * Profiler class for profiling system load.
 */
public final class Profiler {

  private Profiler() {

  }

  /**
   * http://stackoverflow.com/questions/18489273/how-to-get-percentage-of-cpu-usage-of-os-from-java.
   * Get process cpu load
   * @return process cpu load
   */
  public static double getProcessCpuLoad()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
    final AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

    if (list.isEmpty()) {
      return Double.NaN;
    }

    final Attribute att = (Attribute)list.get(0);
    final Double value  = (Double)att.getValue();

    if (value == -1.0) {
      // usually takes a couple of seconds before we get real values
      return Double.NaN;
    }

    // returns a percentage value with 1 decimal point precision
    return ((int)(value * 1000) / 10.0);
  }

  /**
   * Get memory usage of current runtime.
   * @return memory usage
   */
  public static long getMemoryUsage() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  /**
   * Get system cpu load.
   * @return system cpu load.
   */
  public static double getCpuLoad() {
    final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);

    // What % load the overall system is at, from 0.0-1.0
    return osBean.getSystemCpuLoad();
  }

}
