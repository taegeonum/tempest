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
package edu.snu.tempest.operators.common;

import edu.snu.tempest.operators.Timescale;

/**
 * MTSOperator sends MTSWindowOutput to OutputHandler.
 */
public final class MTSWindowOutput<V> {
  /**
   * A timescale for the output.
   */
  public final Timescale timescale;

  /**
   * An output.
   */
  public final V output;

  /**
   * Start time of the output.
   */
  public final long startTime;

  /**
   * End time of the output.
   */
  public final long endTime;

  /**
   * Is this output fully processed or not.
   */
  public final boolean fullyProcessed;
  
  public MTSWindowOutput(final Timescale ts,
                         final V output,
                         final long startTime,
                         final long endTime,
                         final boolean fullyProcessed) {
    this.timescale = ts;
    this.output = output;
    this.startTime = startTime;
    this.endTime = endTime;
    this.fullyProcessed = fullyProcessed;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ts: ");
    sb.append(timescale);
    sb.append(", range: [");
    sb.append(startTime);
    sb.append("-");
    sb.append(endTime);
    sb.append("], output:");
    sb.append(output);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (endTime ^ (endTime >>> 32));
    result = prime * result + ((output == null) ? 0 : output.hashCode());
    result = prime * result + (int) (startTime ^ (startTime >>> 32));
    result = prime * result + ((timescale == null) ? 0 : timescale.hashCode());
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
    final MTSWindowOutput other = (MTSWindowOutput) obj;
    if (endTime != other.endTime) {
      return false;
    }
    if (output == null) {
      if (other.output != null) {
        return false;
      }
    } else if (!output.equals(other.output)) {
      return false;
    }
    if (startTime != other.startTime) {
      return false;
    }
    if (timescale == null) {
      if (other.timescale != null) {
        return false;
      }
    } else if (!timescale.equals(other.timescale)) {
      return false;
    }
    return true;
  }
}
