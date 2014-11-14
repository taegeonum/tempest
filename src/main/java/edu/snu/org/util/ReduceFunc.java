package edu.snu.org.util;

/**
 * Created by Gyewon on 2014. 11. 9..
 */
public interface ReduceFunc<I> {

  public I compute(I value, I sofar);

}