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
package atc.operator.window.timescale;


import atc.operator.Operator;

/**
 * TimescaleWindowOperator interface.
 * It receives input and produces window output every interval.
 */
public interface TimescaleWindowOperator<I, V> extends Operator<I, TimescaleWindowOutput<V>>, AutoCloseable {

  public void addWindow(Timescale ts, long time);

  public void removeWindow(Timescale ts, long time);
}
