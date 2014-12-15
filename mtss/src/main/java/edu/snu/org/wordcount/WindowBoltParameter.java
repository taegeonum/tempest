package edu.snu.org.wordcount;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

public class WindowBoltParameter {

  @NamedParameter
  public static final class WindowLength implements Name<Integer> {}
  
  @NamedParameter
  public static final class SlideInterval implements Name<Integer> {}
}
