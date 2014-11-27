package edu.snu.org.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.security.InvalidParameterException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;

import edu.snu.org.WordCountApp.InputFilePath;

public class LocalInputReader implements InputReader {
  
  /*
   * It doesn't guarantee concurrent read
   */
  private final String inputPath;
  private boolean inputStarted = false;
  private File inputFile;
  private Scanner sc;
  
  @Inject
  public LocalInputReader(@Parameter(InputFilePath.class) String inputPath) {
    
    if (inputPath.length() == 0 || inputPath == null) {
      throw new InvalidParameterException("The input path should not be null");
    }
    
    this.inputPath = inputPath;
  }

  @Override
  public void close() throws Exception {
    sc.close();
  }

  
  @Override
  public String nextLine() {
    
    String input = null;
    // lazy loading. 
    inputStart();
    
    if (sc.hasNextLine()) {
      input = sc.nextLine();
    } else {
      sc.close();

      try {
        sc = new Scanner(inputFile);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
      
      if (sc.hasNextLine()) {
        input = sc.nextLine();
      }
    }
    
    return input;
  }
  
  private void inputStart() {
    if (!inputStarted) {
      inputStarted = true;
      inputFile = new File(inputPath);
      
      if (!inputFile.isFile()) {
        throw new RuntimeException(inputFile + " is not file");
      }
      
      try {
        sc = new Scanner(inputFile);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
