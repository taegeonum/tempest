package org.edu.snu.tempest.examples.utils.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HDFSOutputWriter implements OutputWriter {
  
  private static final Logger LOG = Logger.getLogger(HDFSOutputWriter.class.getName());
  
  private Path path;
  private FileSystem fs;
  private FSDataOutputStream br;
  private Configuration config;
  private boolean started = false;
  
  private final Map<String, FSDataOutputStream> brMap;
  
  @Inject
  public HDFSOutputWriter() {
    this.brMap = new HashMap<>();
  }

  @Override
  public void write(String path, String str) throws IOException {
    
    start();
    
    FSDataOutputStream br = brMap.get(path);
    Path p = null;
    if (br == null) {
      p = new Path(path);
      br = (fs.create(p,true));
      brMap.put(path, br);
    }
    
    br.write(str.getBytes("UTF-8"));
    br.hsync();
  }
  
  @Override
  public void writeLine(String path, String str) throws IOException {
    this.write(path, str + "\n");
  }
  
  private void start() {
    if (!started) {
      started = true;
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
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    for (FSDataOutputStream br : brMap.values()) {
      br.close();
    }
  }

}
