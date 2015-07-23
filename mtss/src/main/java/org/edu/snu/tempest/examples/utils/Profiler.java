package org.edu.snu.tempest.examples.utils;

import com.sun.management.OperatingSystemMXBean;

import javax.management.*;
import java.lang.management.ManagementFactory;

public final class Profiler {

  private Profiler() {

  }

  /**
   * http://stackoverflow.com/questions/18489273/how-to-get-percentage-of-cpu-usage-of-os-from-java.
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
  
  public static long getMemoryUsage() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }
  
  public static double getCpuLoad() {
    final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);

    // What % load the overall system is at, from 0.0-1.0
    return osBean.getSystemCpuLoad();
  }

}
