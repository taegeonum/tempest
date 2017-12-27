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
package vldb.operator.window.timescale.pafas.dynamic;

import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.timescale.TimeMonitor;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.pafas.Node;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/** This implementation is based on "On-the-fly Sharing for Streamed Aggregation" paper.
 * Reference: S. Krishnamurthy, C. Wu, and M. Franklin. On-the-fly sharing
 * for streamed aggregation. In ACM SIGMOD, 2006
 * It returns next slice time for slicing input stream into paired sliced window.
 */
public final class DynamicOptimizedPartialTimespans<T> implements DynamicPartialTimespans<T> {
  private static final Logger LOG = Logger.getLogger(DynamicOptimizedPartialTimespans.class.getName());

  /**
   * The list of timescale.
   */
  private final List<Timescale> timescales;

  /**
   * A start time.
   */
  private final long startTime;

  /**
   * Map of start_time and next timespan map.
   */
  private final ConcurrentMap<Long, Node<T>> partialTimespanMap;

  private final Map<Long, Node<T>> endTimePartialMap;

  private long currIndex;
  private long rebuildSize;

  private final WindowManager windowManager;

  private final TimeMonitor timeMonitor;

  /**
   * StaticComputationReuserImpl Implementation.
   * @param startTime an initial start time
   */
  @Inject
  private DynamicOptimizedPartialTimespans(final WindowManager windowManager,
                                           @Parameter(StartTime.class) final long startTime,
                                           final TimeMonitor timeMonitor) {
    LOG.info("START " + this.getClass());
    this.timescales = windowManager.timescales;
    this.timeMonitor = timeMonitor;
    this.startTime = startTime;
    this.partialTimespanMap = new ConcurrentHashMap<>();
    this.endTimePartialMap = new HashMap<>();
    this.rebuildSize = windowManager.getRebuildSize();
    this.currIndex = startTime + rebuildSize;
    this.windowManager = windowManager;
    buildSlice(startTime, currIndex);
    //System.out.println("CRAET: " + partialTimespanMap);
  }

  @Override
  public Node<T> getNextPartialTimespanNode(final long currTime) {
    //System.out.println("currTime: " + currTime);
    //System.out.println("GET_NEXT_PARTIAL: " + adjStartTime(currTime) +", " + partialTimespanMap.get(adjStartTime(currTime)));
    return partialTimespanMap.get(currTime);
  }

  private void buildSlice(final long start, final long end) {
    long nextStep = end;
    //System.out.println("partial build: " + currIndex + ", "+ nextStep);
    //System.out.println("adjTime null:" + currTime + ", " + adjTime);

    long prevSlice;
    if (start == startTime) {
      prevSlice = start;
    } else {
      final Node<T> prevNode = findBeforeNode(start);
      prevSlice = prevNode.end;
    }

    for (long st = start; st <= end; st += 1) {
      if (slicableByOthers(st)) {
        if (prevSlice < st) {
          //System.out.println("ADD start" + start + ", end: " + end + " (" + prevSlice  +", " + st + ")");
          if (partialTimespanMap.get(prevSlice) == null) {
            //System.out.println("CREATE PARTIAL1: " + prevSlice + ", " + st);
            partialTimespanMap.put(prevSlice, new Node<T>(prevSlice, st, true));
          } else {
            System.out.println("EXIST " + partialTimespanMap.get(prevSlice) + ", expected: " + prevSlice + ", " + st);
            throw new RuntimeException("HAHA");
          }
          prevSlice = st;
        }
      }
    }
    currIndex = nextStep;
  }

  @Override
  public long getNextSliceTime(final long currTime) {
    // TODO: Incremental build
    //System.out.println(partialTimespanMap);
    //System.out.println("currTiume : " + currTime + ", currIndex: " + currIndex + ", rebuildSize: " + rebuildSize);
    if (currTime + rebuildSize > currIndex) {
      final long s = System.nanoTime();
      buildSlice(currIndex, currTime + rebuildSize);
      final long e = System.nanoTime();
      timeMonitor.continuousTime += (e-s);
    }

    Node<T> node = partialTimespanMap.get(currTime);
    if (node == null) {
      // find before node
      final Node<T> before = findBeforeNode(currTime);
      if (before.end > currTime) {
        return before.end;
      }

      /*
      // Find next partial
      for (long i = currTime; i <= currIndex; i++) {
        node = partialTimespanMap.get(i);
        if (node != null) {
          break;
        }
      }
      */
    }
    return node.end;
  }

  private Node<T> findBeforeNode(final long time) {
    for (long i = time; i >=0; i--) {
      final Node<T> node = partialTimespanMap.get(i);
      if (node != null) {
        return node;
      }
    }
    throw new RuntimeException("Not found findBEforeNode: " + time);
  }

  private void nodeSplit(final Node<T> node, final long splitTime) {
    if (node.end > splitTime) {
      final Node<T> newNode = new Node<T>(splitTime, node.end, true);
      //System.out.println("CREATE PARTIAL2: " + splitTime + ", " + node.end);
      partialTimespanMap.put(splitTime, newNode);
      node.end = splitTime;
      final Set<Node<T>> parents = node.parents;
      for (final Node<T> parent : parents) {
        parent.addDependency(newNode);
      }
    }
  }

  @Override
  public void addWindow(final Timescale window, final long prevSliceTime, final long time) {
    // Remove all partial
    //System.out.println("window " + window + " Remove : " + prevSliceTime + ", " + currIndex);
    //System.out.println("Prev ADD " + window + ", " + partialTimespanMap);

    long pairedB = window.windowSize % window.intervalSize;
    final long pairedA = window.intervalSize - pairedB;
    pairedB = pairedB == 0 ? window.intervalSize : pairedB;

    // ADD start time slice
    long st = time;
    boolean odd = true;
    while (st <= currIndex) {
      if (partialTimespanMap.get(st) == null) {
        final Node<T> beforeNode = findBeforeNode(st);
        if (beforeNode.end <= st) {
          // this is end. we need to add new node
          if (beforeNode.end < st) {
            //System.out.println("CREATE PARTIAL3: " + beforeNode.end + ", " + st);
            partialTimespanMap.put(beforeNode.end, new Node<T>(beforeNode.end, st, true));
          }

          long beforeSt = st;
          if (odd) {
            st += pairedA;
            //System.out.println("CREATE PARTIAL3: " + beforeSt + ", " + st + ", pairedA: " + pairedA + ", " + pairedB);
          } else {
            st += pairedB;
            //System.out.println("CREATE PARTIAL3: " + beforeSt + ", " + st + ", pairedA and B: " + pairedA + ", " + pairedB);
          }
          odd = !odd;
          while (st <= currIndex) {
            //System.out.println("CREATE PARTIAL4: " + beforeSt + ", " + st);
            partialTimespanMap.put(beforeSt, new Node<T>(beforeSt, st, true));
            beforeSt = st;
            if (odd) {
              st += pairedA;
            } else {
              st += pairedB;
            }
            odd = !odd;
          }
        } else {
          nodeSplit(beforeNode, st);
        }
      }

      if (odd) {
        st += pairedA;
      } else {
        st += pairedB;
      }
      odd = !odd;
    }

    rebuildSize = windowManager.getRebuildSize();
    if (currIndex < time + rebuildSize) {
      buildSlice(currIndex, time + rebuildSize);
    }
    //System.out.println("Rebuild " + partialTimespanMap);
  }

  private boolean slicableByOthers(final long time) {
    for (final Timescale timescale : timescales) {
      long st = windowManager.timescaleStartTime(timescale);
      long pairedB = timescale.windowSize % timescale.intervalSize;
      final long pairedA = timescale.intervalSize - pairedB;

      if ((time - st) == 0 ||
          (time - st - pairedA) % timescale.intervalSize == 0 ||
          (time - st) % timescale.intervalSize == 0) {
        // this is slicable by other timescale
        //System.out.println("Time " + time + " Slicable by " + timescale + ", at start " + st);
        return true;
      }
    }
    return false;
  }

  private void mergeTwoNode(final Node<T> before, final Node<T> after) {
    //System.out.println("beforeNode: " + before + ", afterNode: " + after);
    before.end = after.end;
    final List<Node<T>> commonParents = new LinkedList<>(before.parents);
    commonParents.retainAll(after.parents);
    for (final Node<T> parent : commonParents) {
      parent.getDependencies().remove(after);
    }
    partialTimespanMap.remove(after.start);
  }

  @Override
  public void removeWindow(final Timescale window, final long prevSliceTime, final long time, final long windowStartTime) {
    // 1) Find partial TS that will be merged
    //System.out.println("BEFORE REMOVE PTS: " + partialTimespanMap);

    // Remove remains
    rebuildSize = windowManager.getRebuildSize();
    if (time + rebuildSize < currIndex) {
      for (long removeIndex = time + rebuildSize + 1; removeIndex <= currIndex; removeIndex += 1) {
        //System.out.println("RM partials: " + removeIndex);
        partialTimespanMap.remove(removeIndex);
      }
      currIndex = time + rebuildSize;
    }
    //System.out.println("partial currIndex: " + currIndex);

    long st = windowStartTime;
    boolean odd = true;
    long pairedB = window.windowSize % window.intervalSize;
    final long pairedA = window.intervalSize - pairedB;
    pairedB = pairedB == 0 ? window.intervalSize : pairedB;

    while (st <= time) {
      if (odd) {
        st += pairedA;
      } else {
        st += pairedB;
      }
      odd = !odd;
    }

    while (st <= currIndex) {
      // If some timescales can split this st, skip
      //System.out.println("partial st: " + st + ", currIndex: " + currIndex);
      if (!slicableByOthers(st)) {
        // Merge!
        final Node<T> node = partialTimespanMap.get(st);
        final Node<T> beforeNode = findBeforeNode(st-1);
        if (node == null) {
          System.out.println("NULL partial: " + st + ", beforeNode: " + beforeNode + ", currIndex: " + currIndex);
          // Remove the before node
          partialTimespanMap.remove(beforeNode);
        } else {
          //System.out.println("MERGE partials: " + beforeNode  + ", after: " + node);
          mergeTwoNode(beforeNode, node);
        }
      }

      if (odd) {
        st += pairedA;
      } else {
        st += pairedB;
      }
      odd = !odd;
    }
  }
  @Override
  public boolean removePartialNode(final long startTime) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean addPartialNode(final long startTime, final long endTime) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void removeNode(final long startTime) {
    partialTimespanMap.remove(startTime);
  }

  @Override
  public String toString() {
    return partialTimespanMap.toString();
  }
}