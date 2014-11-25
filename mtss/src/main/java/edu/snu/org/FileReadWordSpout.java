package edu.snu.org;

import java.io.File;
import java.io.FileNotFoundException;
import java.security.InvalidParameterException;
import java.util.Map;
import java.util.Scanner;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import edu.snu.org.WordCountApp.InputInterval;


/**
 * Read input from files
 */
public class FileReadWordSpout extends BaseRichSpout {

  @NamedParameter(doc = "input file path", short_name = "input")
  public static final class InputPath implements Name<String> {}
  
  SpoutOutputCollector _collector;
  private final int sendingInterval;
  private final String inputPath;
  private File []fileList;  
  private Scanner sc;
  private long startTime;
  private File tempFile;
  
  @Inject
  public FileReadWordSpout(@Parameter(InputInterval.class) int sendingInterval, 
      @Parameter(InputPath.class) String inputPath) {
    
    if (inputPath.length() == 0 || inputPath == null) {
      throw new InvalidParameterException("The input path should not be null");
    }
    
    this.sendingInterval = sendingInterval;
    this.inputPath = inputPath;

  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    
    File dirFile=new File(inputPath);
    fileList=dirFile.listFiles();
    tempFile = fileList[0];
    
    if (!tempFile.isFile()) {
      throw new RuntimeException(tempFile + " is not file");
    }
    
    try {
      sc = new Scanner(tempFile);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {

    if (sc.hasNextLine()) {
      String str = sc.nextLine();
      for(String word: str.split(" ")) {
        _collector.emit(new Values(word, 1, System.currentTimeMillis()));
      }
      Utils.sleep(sendingInterval);
    } else {
      sc.close();
      
      try {
        for(int i = 0; i < fileList.length; i++) {
          File tempFile = fileList[i];
          String tempPath=tempFile.getParent();
          String tempFileName=tempFile.getName();
          System.out.println("Path="+tempPath);
          System.out.println("FileName="+tempFileName);

          if(tempFile.isFile()) {
            sc = new Scanner(tempFile);
            while (sc.hasNextLine()) {
              String str = sc.nextLine();
              
              for(String word: str.split(" ")) {
                _collector.emit(new Values(word, 1, System.currentTimeMillis()));
              }
              Utils.sleep(sendingInterval);
              
            }
            sc.close();
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
      String str = sc.nextLine();
      for(String word: str.split(" ")) {
        _collector.emit(new Values(word, 1, System.currentTimeMillis()));
      }
      Utils.sleep(sendingInterval);
    }
  }

  @Override
  public void close() {
    sc.close();
  }
  
  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
    throw new RuntimeException();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "timestamp"));
  }
}
