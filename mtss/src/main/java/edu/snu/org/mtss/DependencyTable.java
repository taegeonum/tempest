package edu.snu.org.mtss;

import java.util.List;
import java.util.Map;

import edu.snu.org.mtss.DefaultDependencyTableImpl.Range;
import edu.snu.org.util.Timescale;

public interface DependencyTable {

  public Map<Timescale, DTCell> row(long row);
  public long getPeriod();
  public long getBucketSize();
  
  public interface DTCell {
    
    public List<DTCell> getDependencies(); 
    public Object getState();
    public void setState(Object state);
    public Range getRange();
    public boolean isVirtualTimescaleCell();
    public void decreaseRefCnt();
    public int getRefCnt();
  }
}
