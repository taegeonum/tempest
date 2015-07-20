package org.edu.snu.tempest.examples.storm.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.edu.snu.naive.operator.impl.NaiveWindowOperator;
import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.examples.storm.parameters.SavingRate;
import org.edu.snu.tempest.examples.utils.Profiler;
import org.edu.snu.tempest.examples.utils.writer.OutputWriter;
import org.edu.snu.tempest.operator.MTSOperator;
import org.edu.snu.tempest.operator.MTSOperator.Aggregator;
import org.edu.snu.tempest.operator.MTSOperator.OutputHandler;
import org.edu.snu.tempest.operator.WindowOutput;
import org.edu.snu.tempest.signal.MTSSignalReceiver;
import org.edu.snu.tempest.signal.TimescaleSignalListener;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters;
import org.edu.snu.tempest.signal.impl.ZkSignalReceiver;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MTSWordCountTestBolt.
 * It aggregateds word and calculates counts. 
 */
public class MTSWordCountTestBolt extends BaseRichBolt {

  /**
   * WordCount bolt using MTSOperator.
   */
  private static final long serialVersionUID = 1298716785871980572L;
  private static final Logger LOG = Logger.getLogger(MTSWordCountTestBolt.class.getName());


  /**
   * OutputWriter for logging.
   */
  private final OutputWriter writer;

  /**
   * Log directory.
   */
  private final String pathPrefix;

  /**
   * Timescales.
   */
  private final List<Timescale> timescales;

  /**
   * Operator class.
   * [DynamicMTSOperatorImpl, StaticMTSOperatorImpl, NaiveMTSOperatorImpl, OTFMTSOperatorImpl]
   */
  private final Class<? extends MTSOperator> operatorClass;

  private final double savingRate;

  /**
   * Zookeeper address.
   */
  private final String address;

  /**
   * MTS Operator.
   */
  private MTSOperator<Tuple, Map<String, Long>> operator;

  /**
   * Signal Receiver for adding/removing timescales.
   */
  private MTSSignalReceiver receiver;

  /**
   * Scheduled executor for logging.
   */
  private ScheduledExecutorService executor;

  /**
   * Number of execution.
   */
  private final AtomicLong numOfExecution = new AtomicLong();

  private long totalBytes = 0;
  private long sizeOfInt = 4;
  private long sizeOfLong = 8;
  
  public MTSWordCountTestBolt(final OutputWriter writer,
                              final String pathPrefix,
                              final List<Timescale> timescales,
                              final Class<? extends MTSOperator> operatorClass,
                              final String address,
                              final double savingRate) {
    this.writer = writer;
    this.pathPrefix = pathPrefix;
    this.timescales = timescales;
    this.operatorClass = operatorClass;
    this.address = address;
    this.savingRate = savingRate;
    
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer paramOutputFieldsDeclarer) {
  }

  @Override
  public void execute(Tuple tuple) {
    totalBytes += (tuple.getString(0).length() + sizeOfInt + sizeOfLong);
    // send data to MTS operator.
    operator.execute(tuple);
    numOfExecution.incrementAndGet();
  }
  
  
  @Override
  public void prepare(Map conf, TopologyContext paramTopologyContext,
      OutputCollector paramOutputCollector) {
    // profiling 
    this.executor = Executors.newScheduledThreadPool(3);
    this.executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
          try {
            // cpu logging
            writer.writeLine(pathPrefix + "/cpu", (System.currentTimeMillis()) + "\t" 
                + Profiler.getCpuLoad());
            // bytes logging
            writer.writeLine(pathPrefix + "/bytes", (System.currentTimeMillis()) + "\t" 
                + totalBytes);
            // memory logging
            writer.writeLine(pathPrefix + "/memory", (System.currentTimeMillis()) + "\t" 
                + Profiler.getMemoryUsage());
            long executionNum = numOfExecution.get();
            // number of execution
            writer.writeLine(pathPrefix + "/slicedWindowExecution", (System.currentTimeMillis()) + "\t" 
                + executionNum);
            numOfExecution.addAndGet(-1 * executionNum);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          LOG.log(Level.INFO, (System.currentTimeMillis()) + "\t" 
              + totalBytes);
      }
      
    }, 0, 1, TimeUnit.SECONDS);

    // create MTS operator
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(MTSOperator.class, operatorClass);
    cb.bindImplementation(TimescaleSignalListener.class, operatorClass);
    
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, "mts-wcbolt");
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, address);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    cb.bindImplementation(Aggregator.class, CountByKeyAggregator.class);
    cb.bindNamedParameter(SavingRate.class, savingRate+"");

    Injector ij = Tang.Factory.getTang().newInjector(cb.build());

    if (operatorClass.equals(NaiveWindowOperator.class)) {
      // bind one timescale to each executors.
      int index = paramTopologyContext.getThisTaskIndex();
      ij.bindVolatileInstance(Timescale.class, timescales.get(index));
    } else {
      ij.bindVolatileInstance(List.class, timescales);
    }
    ij.bindVolatileInstance(OutputHandler.class, new WCOutputHandler());
    ij.bindVolatileInstance(CountByKeyAggregator.TupleToKey.class, new CountByKeyAggregator.TupleToKey<String>() {
      @Override
      public String getKey(Tuple tuple) {
        return tuple.getString(0);
      }
    });

    try {
      operator = ij.getInstance(MTSOperator.class);
      //receiver = ij.getInstance(MTSSignalReceiver.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
    
    try {
      //receiver.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    operator.start();
  }

  @Override
  public void cleanup() {
    try {
      this.operator.close();
      this.executor.shutdown();
      //this.writer.close();
      //this.receiver.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public final class WCOutputHandler implements OutputHandler<Map<String, Long>> {
    @Inject
    public WCOutputHandler() {
      
    }
    
    @Override
    public void onNext(WindowOutput<Map<String, Long>> output) {
      long count = 0;
      for (Entry<String, Long> entry : output.output.entrySet()) {
        count += entry.getValue();
      }
      
      try {
        writer.writeLine(pathPrefix + "/" + output.timescale.windowSize
            + "-" + output.timescale.intervalSize, (System.currentTimeMillis()) + "\t"
            + count + "\t" + output.elapsedTime);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      LOG.log(Level.INFO, "output " + output.startTime + "-" + output.endTime + ", count: " + count);
    }
  }
}
