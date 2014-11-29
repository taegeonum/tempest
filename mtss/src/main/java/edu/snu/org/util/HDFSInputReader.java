package edu.snu.org.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.tang.annotations.Parameter;

import edu.snu.org.WordCountApp.InputFilePath;

public class HDFSInputReader implements InputReader {
  
  private static final Logger LOG = Logger.getLogger(HDFSInputReader.class.getName());

  private final String inputPath;
  private Path path;
  private FileSystem fs;
  private BufferedReader br;
  private Configuration config;
  private boolean started = false;
  
  @Inject
  public HDFSInputReader(@Parameter(InputFilePath.class) String path) {
    this.inputPath = path;
  }
  
  @Override
  public void close() throws Exception {
    br.close();
  }

  @Override
  public String nextLine() {
    start();
    String line = null;

    try {
      line = br.readLine();
      if (line == null) {
        br.close();
        br= new BufferedReader(new InputStreamReader(fs.open(this.path)));
        line = br.readLine();
      }
    } catch ( IOException e) {
      throw new RuntimeException(e);
    }

    LOG.log(Level.FINEST, "HDFS Input read: " + line);
    return line;
  }

  
  private void start() {
    if (!started) {
      started = true;
      this.path = new Path(inputPath);
      config = new Configuration();
      String hadoop_home = System.getenv("HADOOP_HOME");
      
      if (hadoop_home.length() == 0 || hadoop_home == null) {
        throw new RuntimeException("The env variable HADOOP_HOME is not set");
      }
      
      config.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
      config.addResource(new Path(hadoop_home + "/etc/hadoop/hdfs-site.xml"));
      
      try {

        fs = FileSystem.get(config);
        LOG.log(Level.INFO, "Hadoop configuration: " + config);

        if (this.path == null) {
          throw new NullPointerException("Path should not be null.");
        }

        br= new BufferedReader(new InputStreamReader(fs.open(this.path)));

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
