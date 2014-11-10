package edu.snu.org.mtss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/*
 * Dependency table  
 */
public class MTSDependencyTable<V> {

  private final List<Timescale> timeScales; 
  private final long granularity;
  private final Table<Long, Long, OutputNode> table;
  private final boolean virtualNeeded;
  
  public MTSDependencyTable(List<Timescale> timeScales) throws Exception {
    
    if (timeScales.size() == 0) {
      throw new Exception("MTSOperator should have multiple timescales");
    }
    
    this.timeScales = timeScales;
    this.table = new TreeBasedTableImpl<>(new LongComparator(), new LongInvComparator());
    granularity = calculateGranularitySize();
    virtualNeeded = isVirtualTimescaleNeeded();
    
    createDependencyTable();
  }
  
  
  private void createDependencyTable() {
  
    boolean virtualNeeded = isVirtualTimescaleNeeded(); 

    if (virtualNeeded) {
      // add virtual timescale
      timeScales.add(new Timescale((int)granularity, (int)granularity, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS));
    }
    
    Collections.sort(timeScales);
    
    // add vertex by period
    long period = calculatePeriod();
    int colIndex = 0;
    
    /* Fix: improve algorithm
     * This algorithm add table cells by looping table. 
     * Timescale represents column 
     * time represents row
     */
    for (Timescale ts : timeScales) {
      for(long time = granularity; time <= period; time += granularity) {
        
        if (time % ts.getIntervalSize() == 0) {
          
          // create vertex and add it to the table cell of (time, windowsize)
          OutputNode referer = new OutputNode(time - ts.getWindowSize(), time);
          System.out.println("Created outputnode: " + referer);
          table.put(time, ts.getWindowSize(), referer);
          System.out.println("table size: " + table.size());
          
          // rangeList for determining which vertex is included in this vertex. 
          List<Range> rangeList = new LinkedList<>();
          if (colIndex > 0) {
            rangeList.add(referer.range);
          }
          
          // add edges by scanning all the table cells 
          for (int i = colIndex - 1; i >= 0 && rangeList.size() > 0; i--) {
            Timescale seekTs = timeScales.get(i);
            for (long j = period; j >= granularity && rangeList.size() > 0; j -= granularity) {
              OutputNode referee = table.get(j, seekTs.getWindowSize());
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

                if (includedIdx >= 0) {
                  System.out.println(referer + " references " + referee);
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
   * Determine whether virtual timescale is needed. 
   * If the smallest timescale's window and interval size are equal to granularity size, 
   * then we don't have to create virtual timescale 
   * else we need to create virtual timescale in which the window and interval size is granularity size. 
   */
  private boolean isVirtualTimescaleNeeded() {
    return !(granularity == timeScales.get(0).getWindowSize() && 
        granularity == timeScales.get(0).getIntervalSize());
  }
  
  /*
   * Calculate granularity size of window
   * For example, (w=4, i=2), (w=6,i=4), (w=8,i=6) => granularity size is 2. 
   * We have to add virtual timescale in which window size and interval is equal to the granularity size. 
   * 
   */
  private long calculateGranularitySize() {
    long gcd = 0;
    
    for (Timescale ts : timeScales) {
      if (gcd == 0) { 
        gcd = ts.getIntervalSize();
      } else {
        gcd = gcd(gcd, ts.getIntervalSize());
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
        period = ts.getIntervalSize();
      } else {
        period = lcm(period, ts.getIntervalSize());
      }
      
      // find largest window size
      if (largestWindowSize < ts.getWindowSize()) {
        largestWindowSize = ts.getWindowSize();
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
  
  public Table<Long, Long, OutputNode> getTable() {
    return table;
  }
  
  public long getGranularity() {
    return granularity;
  }

  @Override
  public String toString() {
    
    int sizeofRowCol = 0;
    int sizeOfCell = 40;
    int totalLength = 0;
    
    StringBuilder sb = new StringBuilder();
    sb.append("!sizeOfStartDash\n");
    sb.append("|!sizeOfRowCol");
    
    List<Long> colList = new ArrayList<>();
    colList.addAll(table.columnKeySet());
    Collections.sort(colList, new LongComparator());

    for (Long col : colList) {
      sb.append("| " + col + " " + "!sizeOfCell");
      totalLength += sizeOfCell;
    }
    sb.append("|\n");
    sb.append("!sizeOfStartDash\n");


    
    for (Long row : table.rowKeySet()) {
      String s = "| " + row + " ";
      sizeofRowCol = sizeofRowCol > s.length() ? sizeofRowCol : s.length();
      sb.append(s);
     
      for (Long col : colList) {
        OutputNode n = table.get(row, col);
        
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
  
  public class OutputNode {
    private final List<OutputNode> dependencies; 
    private int refCnt;
    private int initialRefCnt;
    private int iter;
    private V state;
    private final long start;
    private final long end;
    private final Range range;
    
    public OutputNode(long start, long end) {
      this.dependencies = new LinkedList<>();
      refCnt = 0;
      iter = 0;
      this.start = start;
      this.end = end;
      this.range = new Range(start, end);
    }
    
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
    
    public void addDependency(OutputNode n) {
      if (n == null) {
        throw new NullPointerException();
      }
      dependencies.add(n);
      n.increaseRefCnt();
    }
    
    public void increaseRefCnt() {
      refCnt++;
      initialRefCnt = refCnt;
    }
    
    public void decreaseDependenciesRefCnt() {
      
      for (OutputNode referee : dependencies) {
        if (iter == 0) {
          // should not decrease the red line dependencies 
          if (referee.end <= end) {
            referee.decreaseRefCnt();
          }
        } else {
          referee.decreaseRefCnt();
        }
      }
      
      iter++;
    }
   
    public void setState(V state) {
      this.state = state;
    }
    
    public List<OutputNode> getDependencies() {
      return dependencies;
    }
    
    public String toString() {
      return "(ref_cnt: " + refCnt + ", range: " + range + ")";
    }
  }
  
  private class Range {
    private long start;
    private long end;
    
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

class LongInvComparator implements Comparator<Long> {

  @Override
  public int compare(Long o1, Long o2) {
    if (o1 - o2 < 0) {
      return 1;
    } else if (o1 - o2 > 0) {
      return -1;
    } else {
      return 0;
    }
  }
}
