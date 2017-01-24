package atc.operator.window.timescale.triops;

import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.common.Utils;

import javax.inject.Inject;
import java.util.*;

public final class TriWeave {

  @Inject
  private TriWeave() {
  }

  public Set<Group> generateOptimizedPlan(final List<Timescale> timescales) {
    final List<Group> groupedPlans = new LinkedList<>();
    final Map<Group, Map<Group, Double>> costLookupTable = new HashMap<>();

    // Initial cost
    for (final Timescale timescale : timescales) {
      final List<Timescale> list = Arrays.asList(timescale);
      final double cost = calculateTimescaleCost(timescale);
      final Group g = new Group(list, cost);
      groupedPlans.add(g);
      costLookupTable.put(g, new HashMap<Group, Double>());
    }

    while (true) {
      double maxDelta = -10000;
      double mCost = 0.0;
      Group g1 = null;
      Group g2 = null;
      // Find two groups reducing minimum cost.
      for (int i = 0; i < groupedPlans.size()-1; i++) {
        final Group a1 = groupedPlans.get(i);
        for (int j = i+1; j < groupedPlans.size(); j++) {
          final Group a2 = groupedPlans.get(j);
          if (costLookupTable.get(a1).get(a2) == null) {
            // Store the cost for efficiency
            costLookupTable.get(a1).put(a2, mergedCost(a1, a2));
          }
          final double _mCost = costLookupTable.get(a1).get(a2);
          final double delta = (a2.cost + a1.cost) - _mCost;
          //System.out.println("G1: " + a1  + ", G2: " + a2 + ", MERGED_COST: " + _mCost + ", TOTAL: " + (a2.cost+a1.cost));
          if (delta > maxDelta) {
            maxDelta = delta;
            g1 = a1;
            g2 = a2;
            mCost = _mCost;
          }
        }
      }

      if (maxDelta <= 0.0) {
        // End of optimization
        break;
      }

      // Merge the two groups and remove them from groupedPlans
      final boolean removed = groupedPlans.remove(g1);
      //System.out.println("REMOVE " + g1 + ", " + removed);
      groupedPlans.remove(g2);
      //System.out.println("REMOVE " + g2 + ", " + removed);
      final List<Timescale> mergedTs = new LinkedList<>(g1.timescales);
      mergedTs.addAll(g2.timescales);
      final Group mergedGroup = new Group(mergedTs, mCost);
      groupedPlans.add(mergedGroup);
      costLookupTable.remove(g1);
      costLookupTable.remove(g2);
      costLookupTable.put(mergedGroup, new HashMap<Group, Double>());
    }

    return new HashSet<>(groupedPlans);
  }

  private double calculateTimescaleCost(final Timescale timescale) {
    final double edges = timescale.windowSize % timescale.intervalSize == 0 ? 1.0 : 2.0;
    final double edgeRate = edges / timescale.intervalSize;
    return edgeRate + edgeRate * ((timescale.windowSize*(1.0)) / timescale.intervalSize);
  }

  private double calculateEdgeRate(final Timescale timescale) {
    return 2.0 / timescale.intervalSize;
  }

  private double mergedCost(final Group g1, final Group g2) {
    final List<Timescale> merged = new LinkedList<>(g1.timescales);
    merged.addAll(g2.timescales);
    final double edgeRate = getEdgeRate(merged);
    double sum = edgeRate;
    for (final Timescale timescale : merged) {
      sum += edgeRate * ((timescale.windowSize*(1.0)) / timescale.intervalSize);
    }
    return sum;
  }

  private double getEdgeRate(final List<Timescale> timescales) {
    // add sliced window edges
    final long period = Utils.calculatePeriod(timescales);
    final Set<Long> edges = new HashSet<>();
    for (final Timescale ts : timescales) {
      final long pairedB = ts.windowSize % ts.intervalSize;
      final long pairedA = ts.intervalSize - pairedB;
      long time = pairedA;
      boolean odd = true;

      while(time <= period) {
        edges.add(time);
        if (odd) {
          time += pairedB;
        } else {
          time += pairedA;
        }
        odd = !odd;
      }
    }
    return ((1.0)*edges.size()) / period;
  }

  public final class Group {
    public final List<Timescale> timescales;
    final double cost;
    public Group(final List<Timescale> ts,
                 final double cost) {
      this.timescales = ts;
      this.cost = cost;
    }

    @Override
    public String toString() {
      return timescales.toString() + ", " + cost;
    }
  }
}
