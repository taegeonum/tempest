package edu.snu.org;

import java.security.InvalidParameterException;

import backtype.storm.topology.base.BaseRichSpout;
import edu.snu.org.UniqWordCount.UniqWordCountMTSSBuilder;
import edu.snu.org.UniqWordCount.UniqWordCountNaiveBuilder;
import edu.snu.org.grouping.WordGroupingMTSSBuilder;
import edu.snu.org.grouping.WordGroupingNaiveBuilder;
import edu.snu.org.util.HDFSInputReader;
import edu.snu.org.util.HDFSOutputWriter;
import edu.snu.org.util.InputReader;
import edu.snu.org.util.LocalInputReader;
import edu.snu.org.util.LocalOutputWriter;
import edu.snu.org.util.OutputWriter;
import edu.snu.org.wordcount.MTSSTopologyBuilder;
import edu.snu.org.wordcount.NaiveTopologyBuilder;

public class ClassFactory {

  public static final Class<? extends AppTopologyBuilder> createTopologyBuilderClass(String appName) {
    if (appName.compareTo("MTSSTopology") == 0) {
      return MTSSTopologyBuilder.class;
    } else if (appName.compareTo("NaiveTopology") == 0) {
      return NaiveTopologyBuilder.class;
    } else if (appName.compareTo("WordGroupingMTSSTopology") == 0) {
      return WordGroupingMTSSBuilder.class;
    } else if (appName.compareTo("WordGroupingNaiveTopology") == 0) {
      return WordGroupingNaiveBuilder.class;
    } else if (appName.compareTo("UniqWordCountMTSSTopology") == 0) {
      return UniqWordCountMTSSBuilder.class;
    } else if (appName.compareTo("UniqWordCountNaiveTopology") == 0) {
      return UniqWordCountNaiveBuilder.class;
    } else {
      throw new InvalidParameterException("There is no topology builder matched with: " + appName);
    }
  }
  
  public static final Class<? extends BaseRichSpout> createSpoutClass(String spout) {
    if (spout.compareTo("FileReadWordSpout") == 0) {
      return FileReadWordSpout.class;
    } else if (spout.compareTo("RandomWordSpout") == 0) {
      return RandomWordSpout.class;
    } else {
      throw new InvalidParameterException("There is no spout matched with: " + spout);
    }
  }
  
  public static final Class<? extends InputReader> createInputReaderClass(boolean isLocal) {
    if (isLocal) {
      return LocalInputReader.class;
    } else {
      return HDFSInputReader.class;
    }
  }
  
  public static final Class<? extends OutputWriter> createOutputWriterClass(boolean isLocal) {
    if (isLocal) {
      return LocalOutputWriter.class;
    } else {
      return HDFSOutputWriter.class;
    }
  }
}
