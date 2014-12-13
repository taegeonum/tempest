package edu.snu.org.mtss;

import java.util.Collection;

public interface MTSOperator<I, O> {
  
  public void receiveInput(I input);
  public Collection<MTSOutput<O>> flush();
  
  public interface ComputationLogic<I, S> {
    public S computeInitialInput(I input);
    public S computeIntermediate(I input, S state);
    public S computeOutput(Collection<S> states);
  }
}
