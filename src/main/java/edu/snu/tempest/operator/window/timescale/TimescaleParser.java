/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.tempest.operator.window.timescale;

import edu.snu.tempest.operator.window.timescale.parameter.TimescaleString;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Parse timescales from/to string.
 */
public final class TimescaleParser {
  /**
   * Regular expression for string.
   */
  private static final String REGEX = "(\\(\\d+,\\d+\\))*";

  public final List<Timescale> timescales;

  /**
   * Parse timescales from/to string.
   * @param timescaleString string
   */
  @Inject
  private TimescaleParser(@Parameter(TimescaleString.class) final String timescaleString) {
    this.timescales = parseFromString(timescaleString);
  }

  /**
   * Parse string to a list of timescales.
   * @param params string
   * @return a list of timescales
   */
  public static List<Timescale> parseFromString(final String params) {
    if (!params.matches(REGEX)) {
      throw new InvalidParameterException("Invalid timescales: " + params + " The format should be " + REGEX);
    }
    return parseToTimescaleList(params);
  }

  /**
   * Parse timescales to a string.
   * @param timescales a list of timescales
   * @return a string
   */
  public static String parseToString(final List<Timescale> timescales) {
    final StringBuilder sb = new StringBuilder();
    for (Timescale ts : timescales) {
      sb.append("(");
      sb.append(ts.windowSize);
      sb.append(",");
      sb.append(ts.intervalSize);
      sb.append(")");
    }
    return sb.toString();
  }

  private static List<Timescale> parseToTimescaleList(final String params) {
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
