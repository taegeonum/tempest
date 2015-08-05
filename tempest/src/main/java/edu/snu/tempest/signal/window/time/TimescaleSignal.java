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
package edu.snu.tempest.signal.window.time;

/**
 * TimescaleSignal containing timescale and start time information.
 */
public final class TimescaleSignal {
  public static final int ADDITION = 1;
  public static final int DELETION = 2;

  public final long windowSize;
  public final long interval;
  public final int type;
  public final long startTime;

  /**
   * TimescaleSignal for dynamic addition/deletion.
   * @param windowSize a window size of timescale
   * @param interval an interval of timescale
   * @param type of signal : addition/deletion
   * @param startTime start time of the addition/deletion
   */
  public TimescaleSignal(final long windowSize,
                         final long interval,
                         final int type,
                         final long startTime) {
    this.windowSize = windowSize;
    this.interval = interval;
    this.type = type;
    this.startTime = startTime;
  }
}
