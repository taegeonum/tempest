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
package atc.evaluation.util.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writes log into a HDFS file.
 */
public final class HDFSOutputWriter implements OutputWriter {
  private static final Logger LOG = Logger.getLogger(HDFSOutputWriter.class.getName());

  /**
   * A file system.
   */
  private FileSystem fs;

  /**
   * A hadoop configuration.
   */
  private Configuration config;

  /**
   * Is this started.
   */
  private boolean started = false;

  /**
   * A map of output stream.
   */
  private final ConcurrentMap<String, FSDataOutputStream> brMap;

  @Inject
  private HDFSOutputWriter() {
    this.brMap = new ConcurrentHashMap<>();
  }

  /**
   * Write string into a file.
   * @param path a file path
   * @param str a content
   * @throws java.io.IOException
   */
  @Override
  public void write(final String path, final String str) throws IOException {
    initialize();
    FSDataOutputStream br = brMap.get(path);
    final Path p;
    if (br == null) {
      p = new Path(path);
      br = (fs.create(p, true));
      brMap.putIfAbsent(path, br);
      br = brMap.get(path);
    }
    br.write(str.getBytes("UTF-8"));
    br.hsync();
  }

  /**
   * Write string with line into a file.
   * @param path a file path
   * @param str a content
   * @throws java.io.IOException
   */
  @Override
  public void writeLine(final String path, final String str) throws IOException {
    this.write(path, str + "\n");
  }
  
  private void initialize() {
    if (!started) {
      started = true;
      config = new Configuration();
      final String hadoopHome = System.getenv("HADOOP_HOME");
      
      if (hadoopHome.length() == 0 || hadoopHome == null) {
        throw new RuntimeException("The env variable HADOOP_HOME is not set");
      }
      
      config.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
      config.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));
      
      try {
        fs = FileSystem.get(config);
        LOG.log(Level.INFO, "Hadoop configuration: " + config);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    for (final FSDataOutputStream br : brMap.values()) {
      br.close();
    }
  }

}
