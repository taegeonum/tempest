/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package vldb.evaluation.util.writer;

import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writes log into a local file.
 */
public final class LocalOutputWriter implements OutputWriter {
  private static final Logger LOG = Logger.getLogger(LocalOutputWriter.class.getName());

  /**
   * A map of file writer.
   */
  private final ConcurrentMap<String, FileWriter> writerMap;

  private ExecutorService executor;

  private final AtomicBoolean started = new AtomicBoolean(false);

  @Inject
  private LocalOutputWriter() {
    this.writerMap = new ConcurrentHashMap<>();
  }

  @Override
  public void close() throws Exception {
    executor.shutdown();
    for (final FileWriter writer : writerMap.values()) {
      writer.close();
    }
  }

  /**
   * Write string into a file.
   * @param path a file path
   * @param str a content
   * @throws java.io.IOException
   */
  @Override
  public void write(final String path, final String str) throws IOException {
    if (!started.get()) {
      if (started.compareAndSet(false, true)) {
        executor = Executors.newFixedThreadPool(15);
      }
    }

    if (path.length() == 0 || path == null) {
      throw new InvalidParameterException("The output path should not be null");
    }

    FileWriter writer = writerMap.get(path);

    if (writer == null) {
      synchronized (writerMap) {
        writer = writerMap.get(path);
        if (writer == null) {
          createDirectory(path);
          writerMap.putIfAbsent(path, new FileWriter(path, true));
          writer = writerMap.get(path);
        }
      }
    }

    final FileWriter fw = writer;
    executor.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (fw) {
          try {
            fw.write(str);
            fw.flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    });
  }

  /**
   * Write string with line into a file.
   * @param path a file path
   * @param str a content
   * @throws java.io.IOException
   */
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
