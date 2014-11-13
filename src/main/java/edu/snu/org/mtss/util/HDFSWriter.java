package edu.snu.org.mtss.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWriter {
  
  private Path path;
  private FileSystem fs;
  
  public HDFSWriter() {
    
  }
  
  public HDFSWriter(String path) {
    this.path = new Path(path);
    try {
      Configuration config = new Configuration();

      fs = FileSystem.get(new Configuration());
      System.out.println("@@@@ config: " + config);
    } catch (IOException e) {
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
    BufferedWriter br;

    if (fs.exists(path)) {
      br = new BufferedWriter(new OutputStreamWriter(fs.append(path)));
    } else {
      br= new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
      // TO append data to a file, use fs.append(Path f)
    }

    br.write(str);
    br.flush();
    br.close();
  }
  
  public void close() throws IOException {
    this.fs.close();
  }
}
