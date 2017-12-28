package vldb.operator.window.timescale.pafas;

import io.netty.util.internal.ConcurrentSet;
import vldb.operator.window.timescale.Timescale;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
   * DependencyGraphNode.
   */
public final class Node<T> {
  /**
   * A list of dependent nodes.
   */
  private final List<Node<T>> dependencies;

  public final Set<Node<T>> parents;
  /**
   * A reference count to be referenced by other nodes.
   */
  public AtomicInteger refCnt;

  /**
   * An initial reference count.
   */
  public AtomicInteger initialRefCnt;

  /**
   * An output.
   */
  private T output;

  public final Timescale timescale;

  public final AtomicBoolean outputStored = new AtomicBoolean(false);

  public boolean isNotShared = false;

  public int possibleParentCount = 0;

  public long cost;

  public int weight;

  /**
   * The start time of the node.
   */
  public final long start;

  /**
   * The end time of the node.
   */
  public long end;

  public final boolean partial;

  public Node<T> lastChildNode;

  public boolean intermediate;
  /**
   * DependencyGraphNode.
   * @param start the start time of the node.
   * @param end tbe end time of the node.
   */
  public Node(final long start, final long end, boolean partial) {
    this(start, end, partial, null);
  }

  public Node(final long start, final long end, int inter) {
    this.dependencies = new LinkedList<>();
    this.parents = Collections.newSetFromMap(new ConcurrentHashMap<>());
    this.refCnt = new AtomicInteger(0);
    this.initialRefCnt = new AtomicInteger(0);
    this.start = start;
    this.end = end;
    this.partial = false;
    this.intermediate = true;
    this.timescale = null;
  }

  public Node(final long start, final long end, boolean partial, final Timescale ts) {
    this.dependencies = new LinkedList<>();
    this.parents = Collections.newSetFromMap(new ConcurrentHashMap<>());
    this.refCnt = new AtomicInteger(0);
    this.initialRefCnt = new AtomicInteger(0);
    this.start = start;
    this.end = end;
    this.partial = partial;
    this.timescale = ts;
    this.intermediate = false;
  }

  public void reset() {
    dependencies.clear();
    parents.clear();
    refCnt.set(0);
    initialRefCnt.set(0);
  }

  /**
   * For testing. It should not be used.
   */
  public Node(final long start, final long end, final int refCnt, boolean partial) {
    this.dependencies = new LinkedList<>();
    this.parents = new ConcurrentSet<>();
    this.refCnt = new AtomicInteger(refCnt);
    this.start = start;
    this.end = end;
    this.partial = partial;
    this.timescale = null;
    this.intermediate = false;
  }

  /**
   * Decrease reference count of DependencyGraphNode.
   * If the reference count is zero, then it removes the saved output
   * and resets the reference count to initial count
   * in order to reuse this node.
   */
  public void decreaseRefCnt() {
    if (refCnt.get() > 0) {
      int cnt = refCnt.decrementAndGet();
      if (cnt == 0) {
        // Remove output
        final T prevOutput = output;
        output = null;
        outputStored.set(false);
        refCnt.compareAndSet(cnt, initialRefCnt.get());
      }
    }
  }

  /**
   * Add dependent node.
   * @param n a dependent node
   */
  public void addDependency(final Node n) {
    if (n == null) {
      throw new NullPointerException();
    }
    dependencies.add(n);
    if (lastChildNode == null) {
      lastChildNode = n;
    } else {
      if (lastChildNode.end < n.end) {
        lastChildNode = n;
      }
    }


    n.parents.add(this);
    n.increaseRefCnt();
  }

  private void increaseRefCnt() {
    initialRefCnt.incrementAndGet();
    refCnt.incrementAndGet();
  }

  /**
   * Get number of parent nodes.
   * @return number of parent nodes.
   */
  public int getInitialRefCnt() {
    return initialRefCnt.get();
  }

  /**
   * Get child (dependent) nodes.
   * @return child nodes
   */
  public List<Node<T>> getDependencies() {
    return dependencies;
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(start);
    sb.append(", ");
    sb.append(end);
    sb.append(")");
    return sb.toString();
  }

  /*
  public String toString() {
    final boolean outputExists = !(output == null);
    final StringBuilder sb = new StringBuilder();
    sb.append("(init: " + initialRefCnt + ", refCnt: ");
    sb.append(refCnt);
    sb.append(", range: [");
    sb.append(start);
    sb.append("-");
    sb.append(end);
    sb.append("), outputSaved: ");
    sb.append(outputExists);
    sb.append(", #_child: " + dependencies.size() + ")");
    return sb.toString();
  }
  */

  public T getOutput() {
    return output;
  }

  public void saveOutput(final T value) {
    this.output = value;
    this.outputStored.set(true);
  }

}