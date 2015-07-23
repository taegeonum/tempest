package org.edu.snu.tempest.examples.utils.writer;

import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writes log into a local file.
 */
public final class LocalOutputWriter implements OutputWriter {
  private static final Logger LOG = Logger.getLogger(LocalOutputWriter.class.getName());
  
  private final ConcurrentMap<String, FileWriter> writerMap;
  
  @Inject
  public LocalOutputWriter() {
    this.writerMap = new ConcurrentHashMap<>();
    
  }

  @Override
  public void close() throws Exception {
    for (FileWriter writer : writerMap.values()) {
      writer.close();
    }
  }

  @Override
  public void write(final String path, final String str) throws IOException {
    if (path.length() == 0 || path == null) {
      throw new InvalidParameterException("The output path should not be null");
    }
    
    FileWriter writer = writerMap.get(path);

    if (writer == null) {
      createDirectory(path);
      writerMap.putIfAbsent(path, new FileWriter(path));
      writer = writerMap.get(path);
    }

    synchronized (writer) {
      writer.write(str);
      writer.flush();
    }
  }

  @Override
  public void writeLine(final String path, final String str) throws IOException {
    write(path, str + "\n");
  }
  
  
  private void createDirectory(final String dir) {
    
    final String[] splits = dir.split("/");
    final String[] dirSplits = new String[splits.length - 1];
    
    for (int i = 0; i < splits.length - 1; i++) {
      dirSplits[i] = splits[i];
    }
    
    final String d = StringUtils.join(dirSplits, "/");
    final File theDir = new File(d);

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
