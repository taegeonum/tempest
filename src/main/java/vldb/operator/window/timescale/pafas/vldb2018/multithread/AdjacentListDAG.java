/*
 * Copyright (C) 2017 Seoul National University
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
package vldb.operator.window.timescale.pafas.vldb2018.multithread;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implements adjacent list, which will be used to implement DAG.
 * This implementation is not thread-safe.
 * @param <V> vertex type
 * @param <I> edge information type
 */
public final class AdjacentListDAG<V, I> implements DAG<V, I> {
  private static final Logger LOG = Logger.getLogger(AdjacentListDAG.class.getName());

  /**
   * An adjacent list.
   */
  private final Map<V, Map<V, I>> adjacent;

  /**
   * A map for in-degree of vertices.
   */
  private final Map<V, Integer> inDegrees;

  /**
   * The number of edges.
   */
  private int numVertices;

  /**
   * The number of vertices.
   */
  private int numEdges;

  /**
   * A set of root vertices.
   */
  private final Set<V> rootVertices;

  public AdjacentListDAG() {
    this.adjacent = new HashMap<>();
    this.inDegrees = new HashMap<>();
    this.rootVertices = new HashSet<>();
    this.numVertices = 0;
    this.numEdges = 0;
  }

  @Override
  public int numberOfVertices() {
    return numVertices;
  }

  @Override
  public int numberOfEdges() {
    return numEdges;
  }

  @Override
  public Set<V> getRootVertices() {
    return rootVertices;
  }

  @Override
  public Collection<V> getVertices() {
    return adjacent.keySet();
  }

  @Override
  public boolean hasVertex(final V v) {
    return adjacent.get(v) != null;
  }

  @Override
  public boolean isAdjacent(final V v1, final V v2) {
    final Map<V, I> adjs = adjacent.get(v1);
    return adjs.containsKey(v2);
  }

  @Override
  public Map<V, I> getEdges(final V v) {
    final Map<V, I> adjEdges = adjacent.get(v);
    if (adjEdges == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    return adjEdges;
  }

  @Override
  public boolean addVertex(final V v) {
    if (!adjacent.containsKey(v)) {
      adjacent.put(v, new HashMap<>());
      inDegrees.put(v, 0);
      rootVertices.add(v);
      numVertices += 1;
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} already exists", new Object[]{v});
      return false;
    }
  }

  @Override
  public boolean removeVertex(final V v) {
    final Map<V, I> edges = adjacent.remove(v);
    if (edges != null) {
      numVertices -= 1;
      final int inDegreeEdges = inDegrees.remove(v);
      numEdges -= inDegreeEdges;
      numEdges -= edges.size();
      // update inDegrees of neighbor vertices
      // and update rootVertices
      for (final Map.Entry<V, I> edge : edges.entrySet()) {
        final V neighbor = edge.getKey();
        final int inDegree = inDegrees.get(neighbor) - 1;
        inDegrees.put(neighbor, inDegree);
        if (inDegree == 0) {
          rootVertices.add(neighbor);
        }
      }
      rootVertices.remove(v);

      // We have to remove edge that destination vertex is v.
      // This operation is very expensive.
      for (Map.Entry<V, Map<V, I>> entry : adjacent.entrySet()) {
        entry.getValue().remove(v);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} does not exists", new Object[]{v});
      return false;
    }
  }

  @Override
  public boolean addEdge(final V v1, final V v2, final I i) {
    final Map<V, I> adjEdges = getEdges(v1);
    if (adjEdges == null) {
      throw new NoSuchElementException("No src vertex " + v1);
    }

    if (!adjEdges.containsKey(v2)) {
      numEdges += 1;
      adjEdges.put(v2, i);
      final int inDegree = inDegrees.get(v2);
      inDegrees.put(v2, inDegree + 1);
      if (inDegree == 0) {
        rootVertices.remove(v2);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} already exists", new Object[]{v1, v2});
      return false;
    }
  }

  @Override
  public boolean removeEdge(final V v1, final V v2) {
    final Map<V, I> adjEdges = getEdges(v1);
    if (adjEdges == null) {
      throw new NoSuchElementException("No src vertex " + v1);
    }

    if (adjEdges.remove(v2) != null) {
      numEdges -= 1;
      final int inDegree = inDegrees.get(v2);
      inDegrees.put(v2, inDegree - 1);
      if (inDegree == 1) {
        rootVertices.add(v2);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} does not exists", new Object[]{v1, v2});
      return false;
    }
  }

  @Override
  public int getInDegree(final V v) {
    final Integer inDegree = inDegrees.get(v);
    if (inDegree == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    return inDegree;
  }
}
