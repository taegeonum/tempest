package edu.snu.tempest.examples.utils.writer;

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
  public HDFSOutputWriter() {
    this.brMap = new ConcurrentHashMap<>();
  }

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
