package edu.snu.org.util;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;

public class ClusterOutputWriter implements OutputWriter {

  private final String outputPath;
  
  @Inject
  public ClusterOutputWriter(@Parameter(OutputFilePath.class) String path) {
    this.outputPath = path;
    throw new RuntimeException("ClusterOutputWriter is not implemented yet");
  }
  
  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void write(String str) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void writeLine(String str) {
    // TODO Auto-generated method stub
    
  }

}
