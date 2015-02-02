package org.edu.snu.tempest.examples.utils;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.edu.snu.tempest.Timescale;
import org.junit.Test;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TimescaleParserTest {

  @Test
  public void parseSuccessTest() throws InjectionException {
    String str = "(30,2)(40,4)(50,6)(60,7)";
    List<Timescale> list = new ArrayList<>();
    list.add(new Timescale(30, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(40, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(50, 6, TimeUnit.SECONDS, TimeUnit.SECONDS));
    list.add(new Timescale(60, 7, TimeUnit.SECONDS, TimeUnit.SECONDS));

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(TimescaleParser.TimescaleParameter.class, str);
    Injector ij = Tang.Factory.getTang().newInjector(cb.build());

    TimescaleParser tsc = ij.getInstance(TimescaleParser.class);
    List<Timescale> timescales = tsc.timescales;
    
    assert(timescales.equals(list));
  }
  
  @Test
  public void parseInvalidStringTest() throws Exception {
    String invalidStr = "23, 45, 15, 12";
    
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(TimescaleParser.TimescaleParameter.class, invalidStr);
    Injector ij = Tang.Factory.getTang().newInjector(cb.build());

    TimescaleParser tsc = null;
    try {
      tsc = ij.getInstance(TimescaleParser.class);
    } catch (InjectionException e) {
      assert(e.getCause().getClass().equals(InvalidParameterException.class));
    }
    
    if (tsc != null) { 
      throw new Exception("Timescale parser should throw InvalidParameterException");
    }
  }
}
