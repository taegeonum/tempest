package vldb.operator.window.timescale.triops;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.parameter.TimescaleString;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public final class TriWeaveTest {
  private static final Logger LOG = Logger.getLogger(TriWeaveTest.class.getName());

  @Test
  public void testTriWeave() throws InjectionException {
    // Should generate (4,2) {(8,4), (16,4)}, (10,5) plans
    final String tsString = "(4,2)(8,4)(10,5)(16,4)";
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;

    final TriWeave triWeave = injector.getInstance(TriWeave.class);
    final Set<TriWeave.Group> optimizedPlan = triWeave.generateOptimizedPlan(timescales);

    final Set<Set<Timescale>> expected = new HashSet<>();
    expected.add(new HashSet<>(Arrays.asList(new Timescale(4, 2))));
    expected.add(new HashSet<>(Arrays.asList(new Timescale(8, 4), new Timescale(16, 4))));
    expected.add(new HashSet<>(Arrays.asList(new Timescale(10, 5))));

    final Set<Set<Timescale>> result = new HashSet<>();
    for (final TriWeave.Group group : optimizedPlan) {
      result.add(new HashSet<Timescale>(group.timescales));
    }

    LOG.info("GROUPS: " + result);
    Assert.assertEquals(expected, result);
  }
}
