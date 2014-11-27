package edu.snu.org.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;

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
      config = new Configuration(false);
      String hadoop_home = System.getenv("HADOOP_HOME");
      hadoop_home = "/home/hadoop/hadoop-2.4.0";
      config.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
      config.addResource(new Path(hadoop_home + "/etc/hadoop/hdfs-site.xml"));
      //config.addDefaultResource(name);
      try {

        fs = FileSystem.get(config);
        System.out.println("@@@@ config: " + config);


        if (this.path == null) {
          throw new NullPointerException("Path should not be null.");
        }

        /*
        if (fs.exists(this.path)) {
          System.out.println("HDFS bufferedwriter exist");
          br = new BufferedWriter(new OutputStreamWriter(fs.append(this.path)));
        } else {
        
        */
          System.out.println("HDFS bufferedwriter");
          br= new BufferedWriter(new OutputStreamWriter(fs.create(this.path,true)));
          // TO append data to a file, use fs.append(Path f)
        //}
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
  
  public void writeLine(String str) throws IOException {
    // Fix: it didn't work
    System.out.println("HDFS write: " + str);
    br.write(str + "\n");
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
    //br.close();
  }
  
  public static void createDir(String path) {
    File theDir = new File(path);

    // if the directory does not exist, create it
    if (!theDir.exists()) {
      System.out.println("creating directory: " + path);
      boolean result = false;

      try{
        theDir.mkdir();
        result = true;
      } catch(SecurityException se){
        //handle it
      }        
      if(result) {    
        System.out.println("DIR created");  
      }
    }
  }
}
