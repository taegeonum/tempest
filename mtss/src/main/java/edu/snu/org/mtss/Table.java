package edu.snu.org.mtss;

import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

public interface Table<R, C, V> {

  /*
   * Get column values 
   */
  public SortedMap<R, V> column(C columnKey);
  
  /*
   * Get column key set
   */
  public SortedSet<C> columnKeySet();
  
  /*
   * Get column based elements
   */
  public SortedMap<C, SortedMap<R, V>> columnMap();
  
  /*
   * Contain the specific row and column value
   */
  public boolean contains(R rowKey, C columnKey);
  
  
  public boolean containsColumn(C columnKey);
  
  public boolean containsRow(R rowKey);
  
  //public boolean containsValue(V value);
  
  public V get(R rowKey, C columnKey);
  
  public V put(R rowKey, C columnKey, V value);
  
  public SortedMap<C, V> row(R rowKey);
  
  public SortedSet<R> rowKeySet();
  
  public SortedMap<R, SortedMap<C, V>> rowMap();
  
  public int size();
  //public Collection<V> values();
  
  public Set<TableCell<R, C, V>> cellSet();
  
  public interface TableCell<R, C, V> {
    
    public R getRow();
    public C getColumn();
    public V getValue();
  }
}
