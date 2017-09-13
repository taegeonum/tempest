package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import vldb.operator.window.aggregator.CAAggregator;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.timescale.common.Timespan;
import vldb.operator.window.timescale.cutty.CuttyFlatFat;
import vldb.operator.window.timescale.cutty.Fat;
import vldb.operator.window.timescale.parameter.StartTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by taegeonum on 8/25/17.
 */
public final class CuttyFlatFatTest {

  @Test
  public void cuttyFlatFatTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(CAAggregator.class, CountByKeyAggregator.class);
    jcb.bindNamedParameter(CuttyFlatFat.LeafNum.class, "4");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final Fat fat = injector.getInstance(CuttyFlatFat.class);

    System.out.println(Math.ceil(-1.3));
    final Map<String, Long> val1 = new HashMap<>();
    final Map<String, Long> val2 = new HashMap<>();
    final Map<String, Long> val3 = new HashMap<>();
    final Map<String, Long> val4 = new HashMap<>();
    final Map<String, Long> val5 = new HashMap<>();
    final Map<String, Long> val6 = new HashMap<>();

    val1.put("a", 2L);
    val2.put("b", 2L);
    val3.put("a", 3L);
    val4.put("b", 3L);
    val5.put("c", 3L);
    val6.put("d", 3L);

    fat.append(new Timespan(0, 1, null), val1);
    fat.append(new Timespan(1, 2, null), val2);
    fat.append(new Timespan(2, 3, null), val3);

    fat.removeUpTo(2);
    fat.append(new Timespan(3, 4, null), val4);
    fat.append(new Timespan(4, 5, null), val5);

    System.out.println(fat.merge(2));

    fat.append(new Timespan(5, 6, null), val6);

    long s = 0;
    Random r = new Random();
    for (int i = 0; i < 100000000; i++) {
      s = r.nextInt();
    }

    System.out.println(s);

    fat.dump();
  }
}
