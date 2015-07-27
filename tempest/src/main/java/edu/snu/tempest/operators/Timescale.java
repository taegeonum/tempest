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
package edu.snu.tempest.operators;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

/**
 * Represent timescale.
 * Window size and interval
 * Unit: sec
 */
public final class Timescale implements Comparable, Serializable {
  private static final long serialVersionUID = 439658002747284570L;
  public final long windowSize;
  public final long intervalSize;
  
  public Timescale(final int windowSize,
                   final int intervalSize,
                   final TimeUnit windowTimeUnit,
                   final TimeUnit intervalTimeUnit) {
    if (windowSize <= 0 || windowSize - intervalSize < 0) {
      throw new InvalidParameterException("Invalid window or interval size: "
          + "(" + windowSize + ", " + intervalSize + ")");
    }
    this.windowSize = windowTimeUnit.toSeconds(windowSize);
    this.intervalSize = intervalTimeUnit.toSeconds(intervalSize);
  }
  
  public Timescale(final int windowSize, final int intervalSize) {
    this(windowSize, intervalSize, TimeUnit.SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public int compareTo(final Object o) {
    final Timescale tt = (Timescale)o;
    if (windowSize < tt.windowSize) { 
      return -1;
    } else if (windowSize > tt.windowSize) {
      return 1;
    } else {
      return 0;
    }
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (intervalSize ^ (intervalSize >>> 32));
    result = prime * result + (int) (windowSize ^ (windowSize >>> 32));
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Timescale other = (Timescale) obj;
    if (intervalSize != other.intervalSize) {
      return false;
    }
    if (windowSize != other.windowSize) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "[" + windowSize + ", " + intervalSize + "]";
  }
}