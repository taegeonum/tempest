package edu.snu.org.util;

import java.io.Serializable;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.Stage;

public interface OutputWriter extends Serializable, Stage{
  
  @NamedParameter(doc = "output file path", default_value = "")
  public static final class OutputFilePath implements Name<String> {}
  
  public void write(String str);
  public void writeLine(String str);
}
