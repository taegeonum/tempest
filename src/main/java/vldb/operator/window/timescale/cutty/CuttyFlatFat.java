package vldb.operator.window.timescale.cutty;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.parameter.StartTime;

import javax.inject.Inject;
import java.util.*;

/**
 * Created by taegeonum on 8/25/17.
 */
public final class CuttyFlatFat<I, V> implements Fat<V> {

  private final CAAggregator<I, V> aggregator;

  private List<V> heap;
  private int front;
  private int back;

  private int currSize;

  private long prevSlice;

  private int leafNum;

  private Map<Integer, Timespan> indexTimespanMap;

  @NamedParameter(doc = "initial leaf num", default_value = "32")
  public static final class LeafNum implements Name<Integer> {}

  @Inject
  private CuttyFlatFat(@Parameter(StartTime.class) final long startTime,
                       final CAAggregator<I, V> aggregator,
                       @Parameter(LeafNum.class) final int leafNum) {
    this.leafNum = leafNum;
    this.heap = new ArrayList<>(2 * leafNum - 1);
    this.front = leafNum - 1;
    this.back = front ;
    this.indexTimespanMap = new HashMap<>();
    this.prevSlice = startTime;
    this.aggregator = aggregator;
    this.currSize = 0;
    initialize(heap, 2 * leafNum - 1);
  }

  private void initialize(final List<V> list, final int size) {
    for (int i = 0; i < size; i++) {
      list.add(null);
    }
  }

  private int parent(int index) {
    return index % 2 == 0 ? (index / 2) - 1 : index / 2;
  }

  private void heapify(final List<V> list, final V val, final int index) {
    if (index == 0) {
      return;
    }

    list.set(index, val);

    if (index % 2 == 0) {
      // Right heapify
      final int parent = (index / 2) - 1;
      final int leftChild = parent * 2 + 1;
      final V leftChildV = list.get(leftChild);

      V result;
      if (leftChildV == null && val == null) {
        result = null;
        list.set(parent, null);
      } else if (leftChildV == null) {
        result = val;
        list.set(parent, val);
      } else if (val == null) {
        result = leftChildV;
        list.set(parent, leftChildV);
      } else {
        final V comb = aggregator.init();
        aggregator.rollup(comb, leftChildV);
        aggregator.rollup(comb, val);
        list.set(parent, comb);
        result = comb;
      }

      heapify(list, result, parent);
    } else {
      // Left heapify
      final int parent = index / 2;
      final int rightChild = parent * 2 + 2;
      final V rightChildV = list.get(rightChild);

      V result;
      if (rightChildV == null && val == null) {
        result = null;
        list.set(parent, null);
      } else if (rightChildV == null) {
        result = val;
        list.set(parent, val);
      } else if (val == null) {
        result = rightChildV;
        list.set(parent, rightChildV);
      } else {
        final V comb = aggregator.init();
        aggregator.rollup(comb, rightChildV);
        aggregator.rollup(comb, val);
        list.set(parent, comb);
        result = comb;
      }

      heapify(list, result, parent);
    }
  }

  private int increase(final int s, final int size) {
    return s + 1 == size ? leafNum - 1 : s + 1;
  }

  private void doubleSize() {
    final int newLeafNum = 2 * leafNum;
    final List<V> newHeap = new ArrayList<>(newLeafNum - 1);
    final Map<Integer, Timespan> newTimespanMap = new HashMap<>(indexTimespanMap.size());

    initialize(newHeap, 2 * newLeafNum - 1);

    int newHeapIndex = newLeafNum - 1;
    for (int i = front, j = 0; j < currSize; i = increase(i, heap.size()), j++) {
      final V val = heap.get(i);
      final Timespan ts = indexTimespanMap.remove(i);
      newTimespanMap.put(newHeapIndex, ts);
      heapify(newHeap, val, newHeapIndex);

      newHeapIndex += 1;
    }

    front = newLeafNum - 1;
    back = front + currSize;
    heap = newHeap;
    indexTimespanMap = newTimespanMap;
    leafNum = newLeafNum;
  }

  @Override
  public void append(final Timespan timespan, final V partial) {
    if (currSize == leafNum - 1) {
      // Increase!
      doubleSize();
    }

    indexTimespanMap.put(back, timespan);
    heapify(heap, partial, back);
    back = increase(back, heap.size());

    currSize += 1;
  }

  @Override
  public void removeUpTo(final long time) {
    Timespan frontTimespan = indexTimespanMap.get(front);
    while (frontTimespan != null && frontTimespan.endTime <= time) {
      // Make null and heapify
      heapify(heap, null, front);
      front += 1;
      frontTimespan = indexTimespanMap.get(front);
      currSize -= 1;
    }
  }

  private int findIndexStartingFrom(final long from) {
    for (int i = front, j = 0; j < currSize; i = increase(i, heap.size()), j++) {
      final Timespan ts = indexTimespanMap.get(i);
      if (ts != null && ts.startTime == from) {
        return i;
      }
    }
    throw new RuntimeException("Invalid parameter: " + from);
  }

  private boolean isRightChild(final int index) {
    return index % 2 == 0;
  }

  @Override
  public V merge(final long from) {
    int statIndex = findIndexStartingFrom(from);
    int endIndex = back == 0 ? 2 * leafNum - 2 : back - 1;

    final int height = log2(leafNum);

    V leftValue = heap.get(statIndex);
    V rightValue = heap.get(endIndex);

    for (int currH = 1; currH <= height; currH++) {
      final int startParent = parent(statIndex);
      final int endParent = parent(endIndex);

      if (startParent == endParent) {
        final V nb = aggregator.init();
        aggregator.rollup(nb, leftValue);
        aggregator.rollup(nb, rightValue);
        return nb;
      }

      if (!isRightChild(statIndex)) {
        final int rightChildIndex = startParent * 2 + 2;
        final V rightChildValue = heap.get(rightChildIndex);
        final V nb = aggregator.init();
        aggregator.rollup(nb, leftValue);
        aggregator.rollup(nb, rightChildValue);
        leftValue = nb;
      }

      if (isRightChild(endIndex)) {
        final int leftChildIndex = endParent * 2 + 1;
        final V leftChildValue = heap.get(leftChildIndex);
        final V nb = aggregator.init();
        aggregator.rollup(nb, rightValue);
        aggregator.rollup(nb, leftChildValue);
        rightValue = nb;
      }

      statIndex = startParent;
      endIndex = endParent;
    }

    final V nb = aggregator.init();
    aggregator.rollup(nb, leftValue);
    aggregator.rollup(nb, rightValue);
    return nb;
  }

  // log with base 2
  private int log2(int x) {
    if (x == 0) {
      return 0;
    }
    return (int)(Math.log(x) / Math.log(2)); // = log(x) with base 10 / log(2) with base 10
  }

  private String stringOfSize(int size, char ch) {
    char[] a = new char[size];
    Arrays.fill(a, ch);
    return new String(a);
  }

  public void dump() {
    int height = log2(heap.size());

    for (int i = 0, len = heap.size(); i < len; i++) {
      V x = heap.get(i);
      int level = log2(i+1) + 1;
      int spaces = (height - level + 1) * 4;

      System.out.print(stringOfSize(spaces, ' '));
      System.out.print(x);

      if((int)Math.pow(2, level) - 1 == (i+1)) System.out.println();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int max = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < Math.pow(2, i) && j + Math.pow(2, i) < 10; j++) {

        if (j > max) {
          max = j;
        }
      }

    }

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < Math.pow(2, i) && j + Math.pow(2, i) < 10; j++) {

        for (int k = 0; (k < max / ((int) Math.pow(2, i))); k++) {
          sb.append(" ");
        }
        sb.append(heap.get(j + (int) Math.pow(2, i) - 1) + " ");

      }
      sb.append("\n");
    }

    return sb.toString();
  }
}
