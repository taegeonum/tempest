package org.edu.snu.tempest.examples.utils.writer;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.Stage;

import java.io.IOException;
import java.io.Serializable;

/**
 * Output writer for logging.
 */
public interface OutputWriter extends Serializable, Stage{
  
  @NamedParameter(doc = "output file path", default_value = "")
  public static final class OutputFilePath implements Name<String> {}
  void write(String path, String str) throws IOException;
  void writeLine(String path, String str) throws IOException;
}
