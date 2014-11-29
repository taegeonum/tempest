package edu.snu.org.util;

import java.io.Serializable;

import org.apache.reef.wake.Stage;

public interface InputReader extends Serializable, Stage {

  public String nextLine();
  
}
