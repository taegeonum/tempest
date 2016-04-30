package vldb.operator.window.timescale.pafas;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TimescaleString;

import java.util.Arrays;
import java.util.List;

/**
 * Created by taegeonum on 4/21/16.
 */
public final class DependencyGraphTest {

  @Test
     public void testStaticDependencyGraphFinalTimespans() throws InjectionException {
    final String tsString = "(4,2)(5,3)(8,4)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, StaticDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test finalTimespans
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 2 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(2 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 3 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(3 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(0 + i * period, 4 + i * period, timescales.get(0)),
              new Timespan(-4 + i * period, 4 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(4 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(2 + i * period, 6 + i * period, timescales.get(0)),
              new Timespan(1 + i * period, 6 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(6 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 8 + i * period, timescales.get(0)),
              new Timespan(0 + i * period, 8 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(8 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 9 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(9 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(6 + i * period, 10 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(10 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(8 + i * period, 12 + i * period, timescales.get(0)),
              new Timespan(7 + i * period, 12 + i * period, timescales.get(1)),
              new Timespan(4 + i * period, 12 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(12 + i * period));
    }
  }

  @Test
  public void testIncrementalParallelDependencyGraphFinalTimespans() throws InjectionException {
    final String tsString = "(4,2)(5,3)(8,4)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, IncrementalParallelDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test finalTimespans
    for (int i = 0; i < 30; i++) {
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 2 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(2 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 3 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(3 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(0 + i * period, 4 + i * period, timescales.get(0)),
              new Timespan(-4 + i * period, 4 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(4 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(2 + i * period, 6 + i * period, timescales.get(0)),
              new Timespan(1 + i * period, 6 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(6 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 8 + i * period, timescales.get(0)),
              new Timespan(0 + i * period, 8 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(8 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 9 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(9 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(6 + i * period, 10 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(10 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(8 + i * period, 12 + i * period, timescales.get(0)),
              new Timespan(7 + i * period, 12 + i * period, timescales.get(1)),
              new Timespan(4 + i * period, 12 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(12 + i * period));
    }
  }

  @Test
  public void testIncrementalDependencyGraphFinalTimespans() throws InjectionException {
    final String tsString = "(4,2)(5,3)(8,4)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, IncrementalDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test finalTimespans
    for (int i = 0; i < 30; i++) {
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 2 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(2 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 3 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(3 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(0 + i * period, 4 + i * period, timescales.get(0)),
              new Timespan(-4 + i * period, 4 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(4 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(2 + i * period, 6 + i * period, timescales.get(0)),
              new Timespan(1 + i * period, 6 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(6 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 8 + i * period, timescales.get(0)),
              new Timespan(0 + i * period, 8 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(8 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 9 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(9 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(6 + i * period, 10 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(10 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(8 + i * period, 12 + i * period, timescales.get(0)),
              new Timespan(7 + i * period, 12 + i * period, timescales.get(1)),
              new Timespan(4 + i * period, 12 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(12 + i * period));
    }
  }

  @Test
  public void testParallelDependencyGraphFinalTimespans() throws InjectionException {
    final String tsString = "(4,2)(5,3)(8,4)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, StaticParallelDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test finalTimespans
    for (int i = 0; i < 30; i++) {
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 2 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(2 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(-2 + i * period, 3 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(3 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(0 + i * period, 4 + i * period, timescales.get(0)),
              new Timespan(-4 + i * period, 4 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(4 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(2 + i * period, 6 + i * period, timescales.get(0)),
              new Timespan(1 + i * period, 6 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(6 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 8 + i * period, timescales.get(0)),
              new Timespan(0 + i * period, 8 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(8 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(4 + i * period, 9 + i * period, timescales.get(1))),
          dependencyGraph.getFinalTimespans(9 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(6 + i * period, 10 + i * period, timescales.get(0))),
          dependencyGraph.getFinalTimespans(10 + i * period));
      Assert.assertEquals(Arrays.asList(new Timespan(8 + i * period, 12 + i * period, timescales.get(0)),
              new Timespan(7 + i * period, 12 + i * period, timescales.get(1)),
              new Timespan(4 + i * period, 12 + i * period, timescales.get(2))),
          dependencyGraph.getFinalTimespans(12 + i * period));
    }
  }

  @Test
  public void testGreedyStaticDependencyGraph() throws InjectionException {
    final String tsString = "(4,2)(5,3)(6,3)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, StaticDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test greedy algorithm
    for (int i = 0; i < 3; i++) {
      final long j = i * period;
      // [-3,3): [3,4), [-2,3)
      final Node<Integer> n1 = dependencyGraph.getNode(new Timespan(-3 + j, 3 + j, timescales.get(2)));
      final List<Node<Integer>> dn1 = n1.getDependencies();
      Assert.assertEquals(dn1, Arrays.asList(
          dependencyGraph.getNode(new Timespan(3, 4, null)),
          dependencyGraph.getNode(new Timespan(-2, 3, timescales.get(1)))));
      Assert.assertEquals(0, n1.getInitialRefCnt());

      // [0,6): [0,4), [4,6)
      final Node<Integer> n2 = dependencyGraph.getNode(new Timespan(j, 6 + j, timescales.get(2)));
      final List<Node<Integer>> dn2 = n2.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(0, 4, timescales.get(0))),
          dependencyGraph.getNode(new Timespan(4, 6, null))), dn2);
      Assert.assertEquals(0, n2.getInitialRefCnt());

      // [-2, 3): [-2, 2), [2, 3)
      final Node<Integer> n3 = dependencyGraph.getNode(new Timespan(-2 + j, 3 + j, timescales.get(1)));
      final List<Node<Integer>> dn3 = n3.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(-2, 2, timescales.get(0))),
          dependencyGraph.getNode(new Timespan(2, 3, null))), dn3);
      Assert.assertEquals(1, n3.getInitialRefCnt());

      // [1, 6): [1, 2), [2, 6)
      final Node<Integer> n4 = dependencyGraph.getNode(new Timespan(1 + j, 6 + j, timescales.get(1)));
      final List<Node<Integer>> dn4 = n4.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(1, 2, null)),
          dependencyGraph.getNode(new Timespan(2, 6, timescales.get(0)))), dn4);
      Assert.assertEquals(0, n4.getInitialRefCnt());

      // [-2, 2): [4, 6), [0, 1), [1, 2)
      final Node<Integer> n5 = dependencyGraph.getNode(new Timespan(-2 + j, 2 + j, timescales.get(0)));
      final List<Node<Integer>> dn5 = n5.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(4, 6, null)),
          dependencyGraph.getNode(new Timespan(0, 1, null)),
          dependencyGraph.getNode(new Timespan(1, 2, null))), dn5);
      Assert.assertEquals(1, n5.getInitialRefCnt());

      // [0, 4): [0, 1), [1, 2), [2, 3), [3, 4)
      final Node<Integer> n6 = dependencyGraph.getNode(new Timespan(j, 4 + j, timescales.get(0)));
      final List<Node<Integer>> dn6 = n6.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(0, 1, null)),
          dependencyGraph.getNode(new Timespan(1, 2, null)),
          dependencyGraph.getNode(new Timespan(2, 3, null)),
          dependencyGraph.getNode(new Timespan(3, 4, null))), dn6);
      Assert.assertEquals(1, n6.getInitialRefCnt());

      // [2, 6): [2, 3), [3, 4), [4, 6)
      final Node<Integer> n7 = dependencyGraph.getNode(new Timespan(2 + j, 6 + j, timescales.get(0)));
      final List<Node<Integer>> dn7 = n7.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(2, 3, null)),
          dependencyGraph.getNode(new Timespan(3, 4, null)),
          dependencyGraph.getNode(new Timespan(4, 6, null))), dn7);
      Assert.assertEquals(1, n7.getInitialRefCnt());

      // check partial timespans
      // [0, 1), [1, 2), [2, 3), [3, 4), [4, 6)
      final Node<Integer> p1 = dependencyGraph.getNode(new Timespan(0, 1, null));
      Assert.assertEquals(2, p1.getInitialRefCnt());

      final Node<Integer> p2 = dependencyGraph.getNode(new Timespan(1, 2, null));
      Assert.assertEquals(3, p2.getInitialRefCnt());

      final Node<Integer> p3 = dependencyGraph.getNode(new Timespan(2, 3, null));
      Assert.assertEquals(3, p3.getInitialRefCnt());

      final Node<Integer> p4 = dependencyGraph.getNode(new Timespan(3, 4, null));
      Assert.assertEquals(3, p4.getInitialRefCnt());

      final Node<Integer> p5 = dependencyGraph.getNode(new Timespan(4, 6, null));
      Assert.assertEquals(3, p5.getInitialRefCnt());
    }
  }


  @Test
  public void testGreedyIncrementalDependencyGraph() throws InjectionException {
    final String tsString = "(4,2)(5,3)(6,3)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, IncrementalDependencyGraphImpl.class);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();

    // Test greedy algorithm
    for (int i = 0; i < 30; i++) {
      final long j = i * period;
      // [-3,3): [3,4), [-2,3)
      final Node<Integer> n1 = dependencyGraph.getNode(new Timespan(-3 + j, 3 + j, timescales.get(2)));
      final List<Node<Integer>> dn1 = n1.getDependencies();
      Assert.assertEquals(dn1, Arrays.asList(
          dependencyGraph.getNode(new Timespan(3, 4, null)),
          dependencyGraph.getNode(new Timespan(-2, 3, timescales.get(1)))));
      Assert.assertEquals(0, n1.getInitialRefCnt());

      // [0,6): [0,4), [4,6)
      final Node<Integer> n2 = dependencyGraph.getNode(new Timespan(j, 6 + j, timescales.get(2)));
      final List<Node<Integer>> dn2 = n2.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(0, 4, timescales.get(0))),
          dependencyGraph.getNode(new Timespan(4, 6, null))), dn2);
      Assert.assertEquals(0, n2.getInitialRefCnt());

      // [-2, 3): [-2, 2), [2, 3)
      final Node<Integer> n3 = dependencyGraph.getNode(new Timespan(-2 + j, 3 + j, timescales.get(1)));
      final List<Node<Integer>> dn3 = n3.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(-2, 2, timescales.get(0))),
          dependencyGraph.getNode(new Timespan(2, 3, null))), dn3);
      Assert.assertEquals(1, n3.getInitialRefCnt());

      // [1, 6): [1, 2), [2, 6)
      final Node<Integer> n4 = dependencyGraph.getNode(new Timespan(1 + j, 6 + j, timescales.get(1)));
      final List<Node<Integer>> dn4 = n4.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(1, 2, null)),
          dependencyGraph.getNode(new Timespan(2, 6, timescales.get(0)))), dn4);
      Assert.assertEquals(0, n4.getInitialRefCnt());

      // [-2, 2): [4, 6), [0, 1), [1, 2)
      final Node<Integer> n5 = dependencyGraph.getNode(new Timespan(-2 + j, 2 + j, timescales.get(0)));
      final List<Node<Integer>> dn5 = n5.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(4, 6, null)),
          dependencyGraph.getNode(new Timespan(0, 1, null)),
          dependencyGraph.getNode(new Timespan(1, 2, null))), dn5);
      Assert.assertEquals(1, n5.getInitialRefCnt());

      // [0, 4): [0, 1), [1, 2), [2, 3), [3, 4)
      final Node<Integer> n6 = dependencyGraph.getNode(new Timespan(j, 4 + j, timescales.get(0)));
      final List<Node<Integer>> dn6 = n6.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(0, 1, null)),
          dependencyGraph.getNode(new Timespan(1, 2, null)),
          dependencyGraph.getNode(new Timespan(2, 3, null)),
          dependencyGraph.getNode(new Timespan(3, 4, null))), dn6);
      Assert.assertEquals(1, n6.getInitialRefCnt());

      // [2, 6): [2, 3), [3, 4), [4, 6)
      final Node<Integer> n7 = dependencyGraph.getNode(new Timespan(2 + j, 6 + j, timescales.get(0)));
      final List<Node<Integer>> dn7 = n7.getDependencies();
      Assert.assertEquals(Arrays.asList(dependencyGraph.getNode(new Timespan(2, 3, null)),
          dependencyGraph.getNode(new Timespan(3, 4, null)),
          dependencyGraph.getNode(new Timespan(4, 6, null))), dn7);
      Assert.assertEquals(1, n7.getInitialRefCnt());

      // check partial timespans
      // [0, 1), [1, 2), [2, 3), [3, 4), [4, 6)
      final Node<Integer> p1 = dependencyGraph.getNode(new Timespan(0, 1, null));
      Assert.assertEquals(2, p1.getInitialRefCnt());

      final Node<Integer> p2 = dependencyGraph.getNode(new Timespan(1, 2, null));
      Assert.assertEquals(3, p2.getInitialRefCnt());

      final Node<Integer> p3 = dependencyGraph.getNode(new Timespan(2, 3, null));
      Assert.assertEquals(3, p3.getInitialRefCnt());

      final Node<Integer> p4 = dependencyGraph.getNode(new Timespan(3, 4, null));
      Assert.assertEquals(3, p4.getInitialRefCnt());

      final Node<Integer> p5 = dependencyGraph.getNode(new Timespan(4, 6, null));
      Assert.assertEquals(3, p5.getInitialRefCnt());
    }
  }
}
