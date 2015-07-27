package edu.snu.tempest.examples.utils;

import edu.snu.tempest.operators.Timescale;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Parsing timescales from command line.
 */
public final class TimescaleParser {

  public final List<Timescale> timescales;
  private static final String regex = "(\\(\\d+,\\d+\\))*";
  
  @NamedParameter(doc = "timescales. format: (\\(\\d+,\\d+\\))*. TimeUnit: sec",
      short_name = "timescales", default_value = "(30,2)(60,5)(90,6)")
  public static final class TimescaleParameter implements Name<String> {}

  @Inject
  public TimescaleParser(@Parameter(TimescaleParameter.class) final String params) {

    if (!params.matches(regex)) {
      throw new InvalidParameterException("Invalid timescales: " + params + " The format should be " + regex);
    }

    this.timescales = parseToTimescaleList(params);
  }
  
  public long largestWindowSize() {
    return this.timescales.get(this.timescales.size() - 1).windowSize;
  }

  private List<Timescale> parseToTimescaleList(final String params) {
    final List<Timescale> ts = new ArrayList<>();

    // (1,2)(3,4) -> 1,2)3,4)
    final String trim = params.replace("(", "");

    // 1,2)3,4) -> [ "1,2" , "3,4" ] 
    final String[] args = trim.split("\\)");

    for (final String arg : args) {
      final String[] windowAndInterval = arg.split(",");
      ts.add(new Timescale(Integer.valueOf(windowAndInterval[0]),
          Integer.valueOf(windowAndInterval[1]), TimeUnit.SECONDS, TimeUnit.SECONDS));
    }

    return ts;
  }
    
  
}
