package vldb.operator.window.timescale.pafas;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
   * DependencyGraphNode.
   */
public final class Node<T> {
  /**
   * A list of dependent nodes.
   */
  private final List<Node<T>> dependencies;

  public final List<Node<T>> parents;
  /**
   * A reference count to be referenced by other nodes.
   */
  public int refCnt;

  /**
   * An initial reference count.
   */
  private int initialRefCnt;

  /**
   * An output.
   */
  private T output;

  public final AtomicBoolean outputStored = new AtomicBoolean(false);

  /**
   * The start time of the node.
   */
  public final long start;

  /**
   * The end time of the node.
   */
  public long end;

  public final boolean partial;
  /**
   * DependencyGraphNode.
   * @param start the start time of the node.
   * @param end tbe end time of the node.
   */
  public Node(final long start, final long end, boolean partial) {
    this.dependencies = new LinkedList<>();
    this.parents = new LinkedList<>();
    this.refCnt = 0;
    this.initialRefCnt = 0;
    this.start = start;
    this.end = end;
    this.partial = partial;
  }


  /**
   * For testing. It should not be used.
   */
  public Node(final long start, final long end, final int refCnt, boolean partial) {
    this.dependencies = new LinkedList<>();
    this.parents = new LinkedList<>();
    this.refCnt = refCnt;
    this.start = start;
    this.end = end;
    this.partial = partial;
  }

  /**
   * Decrease reference count of DependencyGraphNode.
   * If the reference count is zero, then it removes the saved output
   * and resets the reference count to initial count
   * in order to reuse this node.
   */
  public synchronized void decreaseRefCnt() {
    if (refCnt > 0) {
      refCnt--;
      if (refCnt == 0) {
        // Remove output
        final T prevOutput = output;
        synchronized (prevOutput) {
          output = null;
          outputStored.set(false);
          prevOutput.notifyAll();
        }
        refCnt = initialRefCnt;
      }
    }
  }

  /**
   * Add dependent node.
   * @param n a dependent node
   */
  public synchronized void addDependency(final Node n) {
    if (n == null) {
      throw new NullPointerException();
    }
    dependencies.add(n);
    n.increaseRefCnt();
    n.parents.add(this);
  }

  private synchronized void increaseRefCnt() {
    initialRefCnt++;
    refCnt++;
  }

  /**
   * Get number of parent nodes.
   * @return number of parent nodes.
   */
  public int getInitialRefCnt() {
    return initialRefCnt;
  }

  /**
   * Get child (dependent) nodes.
   * @return child nodes
   */
  public List<Node<T>> getDependencies() {
    return dependencies;
  }

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
    sb.append(")");
    return sb.toString();
  }

  public T getOutput() {
    return output;
  }

  public void saveOutput(final T value) {
    this.output = value;
    this.outputStored.set(true);
  }

}