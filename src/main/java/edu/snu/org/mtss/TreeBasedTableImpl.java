package edu.snu.org.mtss;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class TreeBasedTableImpl<R, C, V> implements Table<R, C, V> {

  private final SortedMap<R, SortedMap<C, V>> rowBasedMap;
  private final SortedMap<C, SortedMap<R, V>> colBasedMap;
  
  private final SortedSet<R> rowKeySet;
  private final SortedSet<C> colKeySet;
  
  private final Comparator<R> rowComparator;
  private final Comparator<C> colComparator;

  private final Set<TableCell<R, C, V>> cellSet;
  
  public TreeBasedTableImpl(Comparator<R> rowComparator, Comparator<C> colComparator) {
  
    this.rowComparator = rowComparator;
    this.colComparator = colComparator;
    
    rowBasedMap = new TreeMap<R, SortedMap<C, V>>(rowComparator);
    colBasedMap = new TreeMap<C, SortedMap<R, V>>(colComparator);

    rowKeySet = new TreeSet<>(rowComparator);
    colKeySet = new TreeSet<>(colComparator);
    
    cellSet = new HashSet<>();
  }



  @Override
  public V put(R row, C col, V val) {
    TableCell<R, C, V> cell = new TableCellImpl(row, col, val);

    if (rowKeySet.contains(row) && colKeySet.contains(col)) {
      // TODO: Warning. duplicated row&col
      SortedMap<C, V> cmap = rowBasedMap.get(row);
      cmap.put(col, val);
      
      SortedMap<R, V> rmap = colBasedMap.get(col);
      rmap.put(row, val);
      
      cellSet.remove(cell);
    } else if (rowKeySet.contains(row) && !colKeySet.contains(col)) {
      SortedMap<C, V> cmap = rowBasedMap.get(row);
      cmap.put(col, val);
      
      SortedMap<R, V> rmap = new TreeMap<>(rowComparator);
      rmap.put(row, val);
      colBasedMap.put(col, rmap);
      
      // add key
      colKeySet.add(col);
      
    } else if (!rowKeySet.contains(row) && colKeySet.contains(col)) {
      SortedMap<R, V> rmap = colBasedMap.get(col);
      rmap.put(row, val);
      
      SortedMap<C, V> cmap = new TreeMap<>(colComparator);
      cmap.put(col, val);
      rowBasedMap.put(row, cmap);
      
      // add key
      rowKeySet.add(row);
      
    } else if (!rowKeySet.contains(row) && !colKeySet.contains(col)) {
      
      SortedMap<R, V> rmap = new TreeMap<>(rowComparator);
      rmap.put(row, val);
      colBasedMap.put(col, rmap);
      
      SortedMap<C, V> cmap = new TreeMap<>(colComparator);
      cmap.put(col, val);
      rowBasedMap.put(row, cmap);
      
      // add key
      colKeySet.add(col);
      rowKeySet.add(row);
    }
    
    cellSet.add(cell);
    return val;
  }
  
  @Override
  public V get(R row, C col) {
    try {
      return rowBasedMap.get(row).get(col);
    } catch(NullPointerException e) {
      return null;
    }
  }
  
  @Override
  public SortedMap<R, V> column(C col) {
    return colBasedMap.get(col);
  }
  
  @Override
  public SortedMap<C, V> row(R row) {
    return rowBasedMap.get(row);
  }

  @Override
  public SortedSet<C> columnKeySet() {
    return colKeySet;
  }

  @Override
  public SortedMap<C, SortedMap<R, V>> columnMap() {
    return colBasedMap;
  }



  @Override
  public boolean contains(R rowKey, C columnKey) {
    return rowKeySet.contains(rowKey) && colKeySet.contains(columnKey);
  }

  @Override
  public boolean containsColumn(C columnKey) {
    return colKeySet.contains(columnKey);
  }

  @Override
  public boolean containsRow(R rowKey) {
    return rowKeySet.contains(rowKey);
  }

  /*
  @Override
  public boolean containsValue(V value) {
    
    for (SortedMap<C, V> cmap : rowBasedMap.values()) {
      for (V v : cmap.values()) {
        if (v.equals(value)) {
          return true;
        }
      }
    }
    
    return false;
  }
  */


  @Override
  public SortedSet<R> rowKeySet() {
    return rowKeySet;
  }

  @Override
  public SortedMap<R, SortedMap<C, V>> rowMap() {
    return rowBasedMap;
  }


  @Override
  public Set<TableCell<R, C, V>> cellSet() {
    return cellSet;
  }
  
  @Override
  public int size() {
    return cellSet.size();
  }


  private class TableCellImpl implements TableCell<R, C, V> {


    public R row;
    public C col;
    public V val;
    
    public TableCellImpl(R row, C col, V val) {
      this.row = row;
      this.col = col;
      this.val = val;
    }

    @Override
    public R getRow() {
      return row;
    }

    @Override
    public C getColumn() {
      return col;
    }

    @Override
    public V getValue() {
      return val;
    }
    
    @Override
    public String toString() {
      return "[R: " + row + ", C: " + col + ", V: " + val + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((col == null) ? 0 : col.hashCode());
      result = prime * result + ((row == null) ? 0 : row.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TableCellImpl other = (TableCellImpl) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (col == null) {
        if (other.col != null)
          return false;
      } else if (!col.equals(other.col))
        return false;
      if (row == null) {
        if (other.row != null)
          return false;
      } else if (!row.equals(other.row))
        return false;
      return true;
    }
    private TreeBasedTableImpl getOuterType() {
      return TreeBasedTableImpl.this;
    }

  }
  
}


