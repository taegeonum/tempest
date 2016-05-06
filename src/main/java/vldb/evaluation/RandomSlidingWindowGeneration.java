package vldb.evaluation;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.evaluation.util.RandomSlidingWindowGenerator;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;

import java.util.Collections;
import java.util.List;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class RandomSlidingWindowGeneration {

  static final int minWindowSize = 200;
  static final int maxWindowSize = 1200;
  static final int minIntervalSize = 1;
  static final int maxIntervalSize = 200;

  public static void main(String[] args) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final RandomSlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    // numWindows
/*
    for (int i = 1; i <= 5; i += 1) {
      final List<Timescale> timescales = swg.generateSlidingWindows(i*20);
      Collections.sort(timescales);
      System.out.println(TimescaleParser.parseToString(timescales));
    }
*/

    //final List<Timescale> timescales = swg.generateSlidingWindows(40);
    //final List<Timescale> timescales = TimescaleParser.parseFromString("(177,27)(247,15)(267,29)(280,10)(283,18)(290,15)(301,10)(329,9)(346,27)(358,3)(373,15)(430,1)(475,30)(477,1)(495,1)(496,3)(506,19)(516,29)(536,19)(544,10)(575,9)(608,30)(613,5)(713,19)(725,10)(758,10)(760,15)(849,10)(901,15)(910,1)(921,19)(926,9)(941,18)(959,30)(1035,3)(1063,19)(1097,6)(1114,3)(1145,18)(1168,6)");
    //Collections.sort(timescales);

    /*
    for (int i = 1; i <= 5; i += 10) {
      final List<Timescale> tss = new LinkedList<>();
      for (final Timescale ts : timescales) {
        tss.add(new Timescale(ts.windowSize, ts.intervalSize*i));
      }
      System.out.println(TimescaleParser.parseToString(tss));
    }
    */



/*
    final List<Integer> intervals = swg.getIntervals(40);

    for (int i = 1; i <= 5; i += 1) {
      final JavaConfigurationBuilder jcb1 = Tang.Factory.getTang().newConfigurationBuilder();
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, (i*200)+"");
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, (i*200 + 200)+"");
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, 10+"");
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, 20+"");

      final Injector injector1 = Tang.Factory.getTang().newInjector(jcb1.build());
      final RandomSlidingWindowGenerator swg1 = injector1.getInstance(RandomSlidingWindowGenerator.class);
      final List<Timescale> timescales = swg1.generateSlidingWindowsWithFixedInterval(intervals);
      Collections.sort(timescales);
      System.out.println(TimescaleParser.parseToString(timescales));
    }
*/



    final List<Integer> windows = swg.getWindows(40);

    for (int i = 1; i <= 5; i += 1) {
      final JavaConfigurationBuilder jcb1 = Tang.Factory.getTang().newConfigurationBuilder();
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, 200+"");
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, 1200+"");
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, Math.max(1, (i*40-40))+"");
      jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, (i*40)+"");

      final Injector injector1 = Tang.Factory.getTang().newInjector(jcb1.build());
      final RandomSlidingWindowGenerator swg1 = injector1.getInstance(RandomSlidingWindowGenerator.class);
      final List<Timescale> timescales = swg1.generateSlidingWindowsWithFixedWindows(windows);
      Collections.sort(timescales);
      System.out.println(TimescaleParser.parseToString(timescales));
    }


  }
}
