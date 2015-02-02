package org.edu.snu.tempest.examples.utils.writer;

import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LocalOutputWriter implements OutputWriter {
  
  /*
   * It doesn't guarantee concurrent write
   */
  
  private static final Logger LOG = Logger.getLogger(LocalOutputWriter.class.getName());
  
  private final Map<String, FileWriter> writerMap;
  
  @Inject
  public LocalOutputWriter() {
    this.writerMap = new HashMap<>();
    
  }

  @Override
  public void close() throws Exception {
    for (FileWriter writer : writerMap.values()) {
      writer.close();
    }
  }


  @Override
  public void write(String path, String str) throws IOException {
    
    if (path.length() == 0 || path == null) {
      throw new InvalidParameterException("The output path should not be null");
    }
    
    FileWriter writer = writerMap.get(path);

    if (writer == null) {
      createDirectory(path);
      writer = new FileWriter(path);
      writerMap.put(path, writer);
    }
    
    writer.write(str);
    writer.flush();
  }

  @Override
  public void writeLine(String path, String str) throws IOException {
    write(path, str + "\n");
  }
  
  
  private void createDirectory(String dir) {
    
    String[] splits = dir.split("/");
    String[] dirSplits = new String[splits.length - 1];
    
    for (int i = 0; i < splits.length - 1; i++) {
      dirSplits[i] = splits[i];
    }
    
    String d = StringUtils.join(dirSplits, "/");
    File theDir = new File(d);

    // if the directory does not exist, create it
    if (!theDir.exists()) {
      LOG.log(Level.INFO, "creating directory: " + d);
      theDir.mkdirs();
      LOG.log(Level.INFO, d +" directory is created");

    } else {
      LOG.log(Level.INFO, "Dir " + d + " is already exist.");
    }
  }

}
