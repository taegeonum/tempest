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

  static final int minWindowSize = 50;
  static final int maxWindowSize = 1200;
  static final int minIntervalSize = 1;
  static final int maxIntervalSize = 30;

  public static void main(String[] args) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, minWindowSize+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, maxWindowSize+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, minIntervalSize+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, maxIntervalSize+"");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final RandomSlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> timescales = swg.generateSlidingWindows(80);
    Collections.sort(timescales);
    System.out.println(TimescaleParser.parseToString(timescales));
  }
}
