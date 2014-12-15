package edu.snu.org.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.tang.annotations.Parameter;

public class HDFSOutputWriter implements OutputWriter {
  
  private static final Logger LOG = Logger.getLogger(HDFSOutputWriter.class.getName());
  
  private Path path;
  private FileSystem fs;
  private FSDataOutputStream br;
  private Configuration config;
  private final String outputPath;
  private boolean started = false;
  
  @Inject
  public HDFSOutputWriter(@Parameter(OutputFilePath.class) String outputPath) {
    this.outputPath = outputPath;
    
  }

  @Override
  public void write(String str) {
    // FIXME: it didn't work
    start();
    LOG.log(Level.INFO, "HDFS Write: " + str);
    
    try {
      br.write(str.getBytes("UTF-8"));
      br.flush();
      br.hsync();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, e.toString());
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void writeLine(String str) {
    write(str + "\n");
  }
  
  private void start() {
    if (!started) {
      started = true;
      this.path = new Path(outputPath);
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

        
        br = (fs.create(this.path,true));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
  }

}
