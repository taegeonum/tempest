/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package atc.evaluation.util;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

/**
 * Class for running Storm.
 */
public final class StormRunner {

  private static final int MILLIS_IN_SEC = 1000;

  private StormRunner() {
  }

  /**
   * Run storm locally.
   * @param topology a topology
   * @param topologyName a topology name
   * @param conf a configuration
   * @param runtimeInSeconds runtime
   * @throws InterruptedException
   */
  public static void runTopologyLocally(final StormTopology topology,
                                        final String topologyName,
                                        final Config conf,
                                        final int runtimeInSeconds)
      throws InterruptedException {
    final LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
    cluster.killTopology(topologyName);
    Thread.sleep(20000);
    cluster.shutdown();
  }

  /**
   * Run storm remotely.
   * @param topology a topology
   * @param topologyName a topology name
   * @param conf a configuration
   * @throws backtype.storm.generated.AlreadyAliveException
   * @throws backtype.storm.generated.InvalidTopologyException
   */
  public static void runTopologyRemotely(final StormTopology topology,
                                         final String topologyName,
                                         final Config conf)
      throws AlreadyAliveException, InvalidTopologyException {
    StormSubmitter.submitTopology(topologyName, conf, topology);
  }
}
