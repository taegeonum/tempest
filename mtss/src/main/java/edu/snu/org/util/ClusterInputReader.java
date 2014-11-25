package edu.snu.org.util;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;

import edu.snu.org.WordCountApp.InputFilePath;

public class ClusterInputReader implements InputReader {

  private final String inputPath;
 
  @Inject
  public ClusterInputReader(@Parameter(InputFilePath.class) String path) {
    this.inputPath = path;
    throw new RuntimeException("ClusterInputReader is not implemented yet");
  }
  
  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String nextLine() {
    // TODO Auto-generated method stub
    return null;
  }

}
