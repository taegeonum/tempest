package atc.operator.window.timescale.pafas;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;
import atc.evaluation.util.RandomSlidingWindowGenerator;
import atc.operator.window.timescale.Timescale;
import atc.operator.window.timescale.pafas.dynamic.DynamicOptimizedPartialTimespans;
import atc.operator.window.timescale.parameter.TimescaleString;

import java.util.HashSet;
import java.util.List;

/**
 * Created by taegeonum on 5/31/16.
 */
public class IncrementalPTTest {

  @Test
  public void testPartialTimespans() throws InjectionException {


    final JavaConfigurationBuilder jcb1 = Tang.Factory.getTang().newConfigurationBuilder();
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, 200+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, 1200+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, 1+"");
    jcb1.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, 15+"");
    //jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxPeriod.class, 10000+"");
    final Injector injector1 = Tang.Factory.getTang().newInjector(jcb1.build());
    final RandomSlidingWindowGenerator swg = injector1.getInstance(RandomSlidingWindowGenerator.class);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final List<Timescale> timescales = swg.generateSlidingWindows(10);
    //jcb.bindNamedParameter(TimescaleString.class, TimescaleParser.parseToString(timescales));
    jcb.bindNamedParameter(TimescaleString.class, "(7,3)(10,5)(20,8)");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final PartialTimespans<Integer> partialTimespans = injector.getInstance(DynamicOptimizedPartialTimespans.class);
    final PartialTimespans<Integer> defaultPT = injector.getInstance(DefaultPartialTimespans.class);
    long time = 0;
    long time2 = 0;
    long period = PeriodCalculator.calculatePeriodFromTimescales(new HashSet<Timescale>(timescales));
    while (time < 40000) {
      long prevTime = time;
      long prevTime2 = time2;
      time2 = defaultPT.getNextSliceTime(prevTime2);
      time = partialTimespans.getNextSliceTime(prevTime);
      Assert.assertEquals(time2, time);
      //System.out.println(prevTime + ", " + time + "," + partialTimespans.getNextPartialTimespanNode(prevTime));
    }
  }
}
