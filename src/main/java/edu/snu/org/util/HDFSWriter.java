package edu.snu.org.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWriter {
  
  private Path path;
  private FileSystem fs;
  private BufferedWriter br;
  private final Configuration config;
  
  public HDFSWriter() {
      config = new Configuration();
      String hadoop_home = System.getenv("HADOOP_HOME");
      config.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
      config.addResource(new Path(hadoop_home + "/etc/hadoop/hdfs-site.xml"));

      try {
        fs = FileSystem.get(config);
      } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public HDFSWriter(String path) {
    this.path = new Path(path);
      config = new Configuration();
      String hadoop_home = System.getenv("HADOOP_HOME");
      config.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
      config.addResource(new Path(hadoop_home + "/etc/hadoop/hdfs-site.xml"));

      try {

        fs = FileSystem.get(config);
        System.out.println("@@@@ config: " + config);


        if (this.path == null) {
          throw new NullPointerException("Path should not be null.");
        }

        if (fs.exists(this.path)) {
          System.out.println("HDFS bufferedwriter exist");
          br = new BufferedWriter(new OutputStreamWriter(fs.append(this.path)));
        } else {
          System.out.println("HDFS bufferedwriter");
          br= new BufferedWriter(new OutputStreamWriter(fs.create(this.path,true)));
          // TO append data to a file, use fs.append(Path f)
        }
      } catch (IOException e) {
        System.out.println(e);
        e.printStackTrace();
      }
  }
  
  public void setPath(String path) {
    this.path = new Path(path);
    try {
      fs = FileSystem.get(new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void write(String str) throws IOException {
    // Fix: it didn't work
    System.out.println("HDFS write: " + str);
    br.write(str);
    br.flush();
  }
  
  public void copyFromLocalFile(Path src, Path dest) throws IOException {
    fs.copyFromLocalFile(src, dest);
  }
  
  public String getDefaultFSName() {
    return config.get("fs.default.name");
  }
  
  public void close() throws IOException {
    //this.fs.close();
    
  }
}
