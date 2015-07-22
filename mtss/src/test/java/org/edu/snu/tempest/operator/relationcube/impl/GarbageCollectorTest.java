package org.edu.snu.tempest.operator.relationcube.impl;

import org.edu.snu.tempest.Timescale;
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
    
    GarbageCollector gc = new DefaultGarbageCollectorImpl(timescales, table, 0);
    gc.onNext(3L);
    verify(table, never()).deleteRow(0);
    gc.onNext(4L);
    verify(table, times(1)).deleteRow(0);
    gc.onNext(5L);
    verify(table, times(1)).deleteRow(1);
    
    gc.onTimescaleAddition(new Timescale(5,1), 5L);
    gc.onNext(6L);
    verify(table, never()).deleteRow(2);
    
    gc.onNext(7L);
    verify(table, never()).deleteRow(2);
    
    gc.onNext(8L);
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
    
    GarbageCollector gc = new DefaultGarbageCollectorImpl(timescales, table, 0);
    gc.onNext(10L);
    verify(table, never()).deleteRow(0);
    gc.onNext(11L);
    verify(table, times(1)).deleteRow(0);
    gc.onNext(12L);
    verify(table, times(1)).deleteRow(1);

    // delete timescale
    gc.onTimescaleDeletion(t2);
    gc.onNext(13L);
    verify(table, times(1)).deleteRow(2);
    verify(table, times(1)).deleteRow(3);
    verify(table, times(1)).deleteRow(4);
    verify(table, times(1)).deleteRow(5);
    verify(table, times(1)).deleteRow(6);
    verify(table, times(1)).deleteRow(7);
    verify(table, times(1)).deleteRow(8);
    verify(table, times(1)).deleteRow(9);
    verify(table, never()).deleteRow(10);

    gc.onNext(14L);
    verify(table, times(1)).deleteRow(10);
  }
}
