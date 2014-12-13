package edu.snu.org.mtss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import edu.snu.org.util.Timescale;

public class DefaultDependencyTableImpl implements DependencyTable {

  private final long bucketSize;
  private final long period;
  
  private final Table<Long, Timescale, DTCell> table;
  private final boolean bucketNeeded;
  private final List<Timescale> timeScales;
  
  @Inject
  public DefaultDependencyTableImpl(List<Timescale> timeScales) {
    this.timeScales = new LinkedList<>(timeScales);
    this.table = new TreeBasedTableImpl<>(new LongComparator(), new TimescaleComparator());
    this.bucketSize = calculateBucketSize();
    this.period = calculatePeriod();
    this.bucketNeeded = isAdditionalBucketTimescaleNeeded();
    
    addTableCells();
  }
 
  
  
  /*
   * This method add vertices and connect dependencies to the table
   * 
   * For example, we have 2 timescales (w=4, i=2), (w=6, i=4) 
   * Then, this method add vertices to the table like this: 
   * 
   * |time\window_size|            2           |           4             |           6             |
   * |       2 sec    | range:(0,2), refCnt: 2 | range:(-2,2), refCnt: 0 |                         |
   * |       4 sec    | range:(2,4), refCnt: 3 | range:(0,4),  refCnt: 1 | range:(-2,4), refCnt:0  |
   * |       6 sec    | range:(4,6), refCnt: 2 | range:(2,6),  refCnt: 0 |                         |
   * |       8 sec    | range:(6,8), refCnt: 3 | range:(4,8),  refCnt: 1 | range:(2,8),  refCnt: 0 |
   * 
   * In this table, the additional timescale (w=2, i=2) is added.
   * Additional timescale is minimum size of timescale in which the window and interval is same. 
   * 
   * Period is 8 sec. 
   * 
   * [2, 2] is referenced by [2,4], [4,4]   (  [row, col] )
   * [4, 2] is referenced by [4,4], [6,4], [8,6]
   * [6, 2] is referenced by [6,4], [8,4]
   * [6, 8] is referenced by [8,4], [2,4], [4,6] 
   * [4, 4] is referenced by [4,6]
   * [8, 4] is referenced by [8,6]
   * 
   */
  private void addTableCells() {
  
    if (bucketNeeded) {
      // add virtual timescale
      timeScales.add(new Timescale((int)bucketSize, (int)bucketSize, TimeUnit.SECONDS, TimeUnit.SECONDS));
    }
    
    Collections.sort(timeScales);
     
    // add vertex by period
    int colIndex = 0;

    for (Timescale ts : timeScales) {
      for(long time = bucketSize; time <= period; time += bucketSize) {
        
        // We need to create vertex when the time is multiplication of interval
        if (time % ts.intervalSize == 0) {
          
          // create vertex and add it to the table cell of (time, windowsize)
          DefaultDTCell referer = new DefaultDTCell(time - ts.windowSize, time, bucketNeeded && (colIndex == 0));
          table.put(time, ts, referer);
          
          // rangeList for determining which vertex is included in this vertex. 
          List<Range> rangeList = new LinkedList<>();
          if (colIndex > 0) {
            rangeList.add(referer.range);
          }
          
          // add edges by scanning all the table cells 
          for (int i = colIndex - 1; i >= 0 && rangeList.size() > 0; i--) {
            Timescale seekTs = timeScales.get(i);
            for (long j = period; j >= bucketSize && rangeList.size() > 0; j -= bucketSize) {
              DefaultDTCell referee = (DefaultDTCell)table.get(j, seekTs);
              if (referee != null) {
                Range seekRg = referee.range;
                
                // is referee included in this vertex?
                int includedIdx = -1;
                for (int idx = 0; idx < rangeList.size(); idx++) {
                  Range rg = rangeList.get(idx);

                  if (rg.include(seekRg) || 
                      ((rg.start < 0) && 
                          new Range(rg.start + period, rg.end + period).include(seekRg))) {
                    includedIdx = idx;
                    break;
                  }
                }

                // connect edge to the vertex in which index is includedIdx
                if (includedIdx >= 0) {
                  Range rg = rangeList.remove(includedIdx);
                  
                  if (rg.include(seekRg)) {
                    rangeList.addAll(rg.splitRange(seekRg));
                  } else {
                    rangeList.addAll(rg.splitRange(new Range(seekRg.start - period, seekRg.end - period)));
                  }
                  referer.addDependency(referee);
                }
              }
            }
          }
        }
      }
      colIndex += 1;
    }
  }
  
  /*
   * Determine whether additional bucket timescale is needed. 
   * If the smallest timescale's window and interval size are equal to the bucket size, 
   * then we don't have to create the additional timescale 
   * else we need to create the additional timescale in which the window and interval size is bucket size. 
   */
  private boolean isAdditionalBucketTimescaleNeeded() {
    return !(bucketSize == timeScales.get(0).windowSize && 
        bucketSize == timeScales.get(0).intervalSize);
  }
  
  /*
   * Calculate bucket size
   * For example, (w=4, i=2), (w=6,i=4), (w=8,i=6) => bucket size is 2. 
   * 
   */
  private long calculateBucketSize() {
    long gcd = 0;
    
    for (Timescale ts : timeScales) {
      if (gcd == 0) { 
        gcd = gcd(ts.windowSize, ts.intervalSize);
      } else {
        gcd = gcd(gcd, ts.intervalSize);
      }
    }
    
    return gcd;
  }
  
  
  /*
   * Find period of repeated pattern 
   * period = c * lcm ( i_{1}, i_{2}, ..., i_{k} ) ( i_{k} is interval of k-th timescale) 
   * c is natural number which satisfies period >= largest_window_size
   */
  private long calculatePeriod() {
    
    long period = 0;
    long largestWindowSize = 0;
    
    for (Timescale ts : timeScales) {
      if (period == 0) {
        period = ts.intervalSize;
      } else {
        period = lcm(period, ts.intervalSize);
      }
      
      // find largest window size
      if (largestWindowSize < ts.windowSize) {
        largestWindowSize = ts.windowSize;
      }
    }
    
    if (period < largestWindowSize) {
      long div = largestWindowSize / period;
      
      if (largestWindowSize % period == 0) {
        period *= div;
      } else {
        period *= (div+1);
      }
    }
    
    return period;
  }

  private static long gcd(long a, long b) {
    while (b > 0) {
      long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }

  private static long lcm(long a, long b)
  {
    return a * (b / gcd(a, b));
  }
  

  @Override
  public Map<Timescale, DTCell> row(long row) {
    return table.row(row);
  }


  /*
   * For debugging 
   * 
   */
  @Override
  public String toString() {
    
    int sizeofRowCol = 0;
    int sizeOfCell = 40;
    int totalLength = 0;
    
    StringBuilder sb = new StringBuilder();
    sb.append("!sizeOfStartDash\n");
    sb.append("|!sizeOfRowCol");
    
    List<Timescale> colList = new ArrayList<>();
    colList.addAll(table.columnKeySet());
    Collections.sort(colList, new TimescaleComparator());

    for (Timescale col : colList) {
      sb.append("| " + col.windowSize + " " + "!sizeOfCell");
      totalLength += sizeOfCell;
    }
    sb.append("|\n");
    sb.append("!sizeOfStartDash\n");


    
    for (Long row : table.rowKeySet()) {
      String s = "| " + row + " ";
      sizeofRowCol = sizeofRowCol > s.length() ? sizeofRowCol : s.length();
      sb.append(s);
     
      for (Timescale col : colList) {
        DTCell n = table.get(row, col);
        
        if (n == null) {
          sb.append(fixedLengthString("| ", "",sizeOfCell));
        } else {
          sb.append(fixedLengthString("| " + n + " ", sizeOfCell));
        }
        
      }

      sb.append("\n");
      sb.append("!sizeOfStartDash\n");
    }

    String result = sb.toString();

    result = result.replace("!sizeOfStartDash", fillWithChar('-', totalLength));
    result = result.replace("!sizeOfRowCol", fillWithChar(' ', sizeofRowCol));
    result = result.replace("!sizeOfCell", fillWithChar(' ', sizeOfCell - 7));
    return result;
  }

  public static String fixedLengthString(String string, int length) {
    return String.format("%1$"+length+ "s", string);
  }
  
  public static String fixedLengthString(String prefix, String string, int length) {
    return String.format("%1s%2$"+length+ "s", prefix, string);
  }
  
  private String fillWithChar(Character c, int len) {
    StringBuffer outputBuffer = new StringBuffer(len);
    for (int i = 0; i < len; i++){
       outputBuffer.append(c);
    }
    
    return outputBuffer.toString();
  }
 

  public class DefaultDTCell implements DTCell {
    private final List<DTCell> dependencies; 
    private int refCnt;
    private int initialRefCnt;
    private Object state;
    private final long start;
    private final long end;
    private final Range range;
    private final boolean isVirtual;

    public DefaultDTCell(long start, long end, boolean isVirtual) {
      this.dependencies = new LinkedList<>();
      refCnt = 0;
      this.start = start;
      this.end = end;
      this.range = new Range(start, end);
      this.isVirtual = isVirtual;
    }

    @Override
    public void decreaseRefCnt() {
      if (refCnt > 0) {
        refCnt--;

        if (refCnt == 0) {
          // Remove state
          state = null;
          refCnt = initialRefCnt;
        }
      }
    }

    private void addDependency(DTCell n) {
      if (n == null) {
        throw new NullPointerException();
      }
      dependencies.add(n);
      ((DefaultDTCell)n).increaseRefCnt();
    }

    private void increaseRefCnt() {
      refCnt++;
      initialRefCnt = refCnt;
    }

    @Override
    public int getRefCnt() {
      return refCnt;
    }

    @Override
    public List<DTCell> getDependencies() {
      return dependencies;
    }

    public String toString() {
      boolean isState = !(state == null);
      return "(refCnt: " + refCnt + ", range: " + range + ", s: " + isState + ")";
    }

    @Override
    public Object getState() {
      return state;
    }

    @Override
    public void setState(Object state) {
      this.state = state;
    }

    @Override
    public Range getRange() {
      return range;
    }

    @Override
    public boolean isVirtualTimescaleCell() {
      return isVirtual;
    }
  }


  public class Range {
    public final long start;
    public final long end;

    public Range(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public boolean include(Range r) {
      if (r.start >= start && r.end <= end) {
        return true;
      } else {
        return false;
      }
    }

    public List<Range> splitRange(Range r) {
      List<Range> list = new LinkedList<>();
      if (r.start > start) {
        list.add(new Range(start, r.start));
      } 

      if (r.end < end) {
        list.add(new Range(r.end, end));
      }
      return list;
    }

    @Override
    public String toString() {
      return "(" + start + ", " + end + ")";
    }
  }


  @Override
  public long getPeriod() {
    return period;
  }



  @Override
  public long getBucketSize() {
    return bucketSize;
  }

}



class LongComparator implements Comparator<Long> {

  @Override
  public int compare(Long o1, Long o2) {
    if (o1 - o2 < 0) {
      return -1;
    } else if (o1 - o2 > 0) {
      return 1;
    } else {
      return 0;
    }
  }
  
}

class TimescaleComparator implements Comparator<Timescale> {
  
  @Override
  public int compare(Timescale t1, Timescale t2) {
    if (t1.windowSize - t2.windowSize < 0) {
      return -1;
    } else if (t1.windowSize - t2.windowSize > 0) {
      return 1;
    } else {
      return 0;
    }
  }
}