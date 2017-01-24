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
package atc.operator.window.timescale.pafas.dynamic;

import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.pafas.PartialTimespans;

/** This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It returns next slice time for slicing input stream into paired sliced window.
 */
public interface DynamicPartialTimespans<T> extends PartialTimespans {

  public void addWindow(final Timescale window, final long prevSliceTime, final long time);

  public void removeWindow(final Timescale window, final long prevSliceTime, final long time, long windowStartTime);

  public void removeNode(final long startTime);
}