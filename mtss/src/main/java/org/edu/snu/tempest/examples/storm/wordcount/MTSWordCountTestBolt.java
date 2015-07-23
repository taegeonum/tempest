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
import org.edu.snu.onthefly.operator.impl.OTFMTSOperatorImpl;
import org.edu.snu.tempest.examples.utils.Profiler;
import org.edu.snu.tempest.examples.utils.writer.OutputWriter;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.common.Aggregator;
import org.edu.snu.tempest.operators.common.WindowOutput;
import org.edu.snu.tempest.operators.common.aggregators.CountByKeyAggregator;
import org.edu.snu.tempest.operators.dynamicmts.impl.DynamicMTSOperatorImpl;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignalListener;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkSignalReceiver;
import org.edu.snu.tempest.operators.parameters.CachingRate;
import org.edu.snu.tempest.operators.parameters.InitialStartTime;
import org.edu.snu.tempest.operators.staticmts.MTSOperator;
import org.edu.snu.tempest.operators.staticmts.impl.StaticMTSOperatorImpl;

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
   * Operators class.
   * [mts, naive, rg, otf]
   */
  private final String operatorName;

  private final double cachingRate;

  /**
   * Zookeeper address.
   */
  private final String address;

  /**
   * MTS Operator.
   */
  private MTSOperator<Tuple> operator;

  /**
   * Scheduled executor for logging.
   */
  private ScheduledExecutorService executor;

  /**
   * Number of execution.
   */
  private final AtomicLong numOfExecution = new AtomicLong();

  private final long startTime;

  private long totalBytes = 0;
  private long sizeOfInt = 4;
  private long sizeOfLong = 8;
  
  public MTSWordCountTestBolt(final OutputWriter writer,
                              final String pathPrefix,
                              final List<Timescale> timescales,
                              final String operatorName,
                              final String address,
                              final double cachingRate,
                              final long startTime) {
    this.writer = writer;
    this.pathPrefix = pathPrefix;
    this.timescales = timescales;
    this.operatorName = operatorName;
    this.address = address;
    this.cachingRate = cachingRate;
    this.startTime = startTime;
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
    cb.bindImplementation(Aggregator.class, CountByKeyAggregator.class);
    cb.bindNamedParameter(CachingRate.class, cachingRate + "");
    cb.bindNamedParameter(InitialStartTime.class, this.startTime + "");

    if (operatorName.equals("mts")) {
      cb.bindImplementation(MTSOperator.class, DynamicMTSOperatorImpl.class);
      cb.bindImplementation(TimescaleSignalListener.class, DynamicMTSOperatorImpl.class);
      cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, "mts-wcbolt");
      cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, address);
      cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    } else if (operatorName.equals("naive")) {
      cb.bindImplementation(MTSOperator.class, NaiveWindowOperator.class);
    } else if (operatorName.equals("rg")) {
      cb.bindImplementation(MTSOperator.class, StaticMTSOperatorImpl.class);
    } else if (operatorName.equals("otf")) {
      cb.bindImplementation(MTSOperator.class, OTFMTSOperatorImpl.class);
    } else {
      throw new RuntimeException("Operator exception: " + operator);
    }

    Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    if (operatorName.equals("naive")) {
      // bind one timescale to each executors.
      int index = paramTopologyContext.getThisTaskIndex();
      ij.bindVolatileInstance(Timescale.class, timescales.get(index));
    } else {
      ij.bindVolatileInstance(List.class, timescales);
    }
    ij.bindVolatileInstance(MTSOperator.OutputHandler.class, new WCOutputHandler());
    ij.bindVolatileInstance(CountByKeyAggregator.KeyFromValue.class,
        new CountByKeyAggregator.KeyFromValue<Tuple, String>() {
          @Override
          public String getKey(Tuple tuple) {
            return tuple.getString(0);
          }
        });

    try {
      operator = ij.getInstance(MTSOperator.class);
    } catch (InjectionException e) {
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public final class WCOutputHandler implements MTSOperator.OutputHandler<Map<String, Long>> {
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
      LOG.log(Level.INFO, "output of ts" + output.timescale + ": "
          + output.startTime + "-" + output.endTime + ", count: " + count);
    }
  }
}
