package org.edu.snu.tempest.operator.relationcube.impl;

import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.operator.impl.LogicalTime;
import org.edu.snu.tempest.operator.relationcube.GarbageCollector;
import org.edu.snu.tempest.operator.relationcube.OutputLookupTable;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

public class GarbageCollectorTest {

  @Test
  public void onTimescaleAdditionTest() {
    List<Timescale> timescales = new LinkedList<>();
    timescales.add(new Timescale(3,1));
    
    OutputLookupTable<?> table = mock(OutputLookupTable.class);
    
    GarbageCollector gc = new DefaultGarbageCollectorImpl(timescales, table);
    gc.onNext(new LogicalTime(3));
    verify(table, never()).deleteRow(0);
    gc.onNext(new LogicalTime(4));
    verify(table, times(1)).deleteRow(0);
    gc.onNext(new LogicalTime(5));
    verify(table, times(1)).deleteRow(1);
    
    gc.onTimescaleAddition(new Timescale(5,1));
    gc.onNext(new LogicalTime(6));
    verify(table, never()).deleteRow(2);
    
    gc.onNext(new LogicalTime(7));
    verify(table, never()).deleteRow(2);
    
    gc.onNext(new LogicalTime(8));
    verify(table, times(1)).deleteRow(2);
  }
  
  @Test
  public void onTimescaleDeletionTest() {
    List<Timescale> timescales = new LinkedList<>();
    Timescale t1 = new Timescale(3,1);
    Timescale t2 = new Timescale(10,2);
    
    timescales.add(t1);
    timescales.add(t2);
    
    OutputLookupTable<?> table = mock(OutputLookupTable.class);
    
    GarbageCollector gc = new DefaultGarbageCollectorImpl(timescales, table);
    gc.onNext(new LogicalTime(10));
    verify(table, never()).deleteRow(0);
    gc.onNext(new LogicalTime(11));
    verify(table, times(1)).deleteRow(0);
    gc.onNext(new LogicalTime(12));
    verify(table, times(1)).deleteRow(1);

    // delete timescale
    gc.onTimescaleDeletion(t2);
    gc.onNext(new LogicalTime(13));
    verify(table, times(1)).deleteRow(2);
    verify(table, times(1)).deleteRow(3);
    verify(table, times(1)).deleteRow(4);
    verify(table, times(1)).deleteRow(5);
    verify(table, times(1)).deleteRow(6);
    verify(table, times(1)).deleteRow(7);
    verify(table, times(1)).deleteRow(8);
    verify(table, times(1)).deleteRow(9);
    verify(table, never()).deleteRow(10);

    gc.onNext(new LogicalTime(14));
    verify(table, times(1)).deleteRow(10);
  }
}
