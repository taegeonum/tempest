package edu.snu.org.util;

import java.util.List;
import java.util.Map;

public class WordCountUtils {
  
  public static StartTimeAndTotalCount getAverageStartTimeAndTotalCount(Map<String, ValueAndTimestamp<Integer>> map) {
    
    long totalCnt = 0;
    long avgStartTime = 0;
    for (ValueAndTimestamp<Integer> val : map.values()) {
      long temp = totalCnt + val.value;

      if (temp > 0) {
        avgStartTime = (long)((totalCnt * avgStartTime) / (double)temp + val.timestamp / (double)temp);
      }
      
      totalCnt = temp;
    }
    
    return new StartTimeAndTotalCount(avgStartTime, totalCnt);
  }
  
  public static class StartTimeAndTotalCount {
    public final long avgStartTime;
    public final long totalCount;
    
    private StartTimeAndTotalCount(final long avgStartTime, final long totalCount) {
      this.avgStartTime = avgStartTime;
      this.totalCount = totalCount;
    }
  }
  
  public static long tickTime(List<Timescale> timescales) {
    return calculateBucketSize(timescales);
  }
  
  
  /*
   * Calculate bucket size
   * For example, (w=4, i=2), (w=6,i=4), (w=8,i=6) => bucket size is 2. 
   * 
   */
  private static long calculateBucketSize(List<Timescale> timescales) {
    long gcd = 0;
    
    for (Timescale ts : timescales) {
      if (gcd == 0) { 
        gcd = gcd(ts.windowSize, ts.intervalSize);
      } else {
        gcd = gcd(gcd, ts.intervalSize);
      }
    }
    
    return gcd;
  }
  

  private static long gcd(long a, long b) {
    while (b > 0) {
      long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }
  
}
