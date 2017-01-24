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
import atc.operator.window.timescale.pafas.DependencyGraph;
import atc.operator.window.timescale.pafas.Node;

/**
 * DependencyGraph shows which nodes are related to each other. This is used so that unnecessary outputs are not saved.
 */
public interface DynamicDependencyGraph<T> extends DependencyGraph {

  public void removeSlidingWindow(final Timescale ts, final long tsStartTime, final long deleteTime);

  public void removeNode(final Node<T> node);

}
