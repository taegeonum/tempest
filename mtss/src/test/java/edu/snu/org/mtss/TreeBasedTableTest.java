package edu.snu.org.mtss;

import java.util.Set;

import org.junit.Test;

import edu.snu.org.mtss.Table.TableCell;

public class TreeBasedTableTest {

  @Test
  public void setCellValueTest() {
    Long i1 = 1L;
    Long i2 = 2L;
    String v1 = "abc";
    
    Table<Long, Long, String> table = new TreeBasedTableImpl<>(new LongComparator(), new LongComparator());
    Set<TableCell<Long, Long, String>> cells = table.cellSet();
    assert(cells.size() == 0);
   
    table.put(i1, i2, v1);
    assert(cells.size() == 1);
    
    String v = table.get(i1, i2);

    assert(v.equals(v1));
    
  }
}
