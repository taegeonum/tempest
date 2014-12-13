package edu.snu.org.mtss;


public class MTSOutput<O> {

  public final long time;
  public final long sizeOfWindow;
  public final O result;
  
  public MTSOutput(final long time, final long sizeOfWindow, final O result) {
    this.time = time;
    this.sizeOfWindow = sizeOfWindow;
    this.result = result;
  }
  
  @Override
  public String toString() {
    return "{" + time + ", " + sizeOfWindow + ", " + result + "}";
  }
}
