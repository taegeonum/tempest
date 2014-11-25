package edu.snu.org.util;

import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;

public class LocalOutputWriter implements OutputWriter {
  
  private final String outputPath;
  private final AtomicBoolean outputStarted = new AtomicBoolean(false);
  private FileWriter outputWriter;
  
  @Inject
  public LocalOutputWriter(@Parameter(OutputFilePath.class) String outputPath) {
    
    if (outputPath.length() == 0 || outputPath == null) {
      throw new InvalidParameterException("The output path should not be null");
    }
    
    this.outputPath = outputPath;
    
  }

  @Override
  public void close() throws Exception {
    outputWriter.close();
  }


  @Override
  public void write(String str) {
    outputStart();
    
    try {
      outputWriter.write(str);
      outputWriter.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeLine(String str) {
    write(str + "\n");
  }
  
  private void outputStart() {
    
    if (outputStarted.compareAndSet(false, true)) {
      try {
        outputWriter = new FileWriter(outputPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

}
