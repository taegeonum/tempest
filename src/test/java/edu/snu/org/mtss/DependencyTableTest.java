package edu.snu.org.mtss;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class DependencyTableTest {
  
  
  @Test
  public void createTest() throws Exception {
    
    List<Timescale> timescales = new LinkedList<>();
    //timescales.add(new Timescale(2, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    //timescales.add(new Timescale(4, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    //timescales.add(new Timescale(8, 4, TimeUnit.SECONDS, TimeUnit.SECONDS));

    timescales.add(new Timescale(3, 2, TimeUnit.SECONDS, TimeUnit.SECONDS));
    timescales.add(new Timescale(12, 5, TimeUnit.SECONDS, TimeUnit.SECONDS));

    MTSDependencyTable table = new MTSDependencyTable(timescales);
    
    System.out.println(table);
    
  }
}
