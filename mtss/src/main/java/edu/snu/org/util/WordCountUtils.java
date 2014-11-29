package edu.snu.org.util;

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
}
