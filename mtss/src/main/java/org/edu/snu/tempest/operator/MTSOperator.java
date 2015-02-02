package org.edu.snu.tempest.operator;

import org.edu.snu.tempest.Timescale;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;
import org.edu.snu.tempest.signal.TimescaleSignalListener;
import org.edu.snu.tempest.examples.utils.DefaultOutputHandler;

import java.util.List;

/*
 * MTS operator interface 
 * It dynamically produces multi-time scale outputs whenever it receives a new timescale or removes an existing timescale. 
 */
public interface MTSOperator<I, V> extends TimescaleSignalListener, Stage {

  /*
   * Aggregation function
   */
  public interface Aggregator<I, V> {
    
    /*
     * Initialization function when the data is new
     *
     */
    public V init();
    
    /*
     * Aggregate function when the existing key is aggregated 
     * 
     * @param oldVal old value
     * @param newVal new value
     */
    public V partialAggregate(final V oldVal, final I newVal);

    /**
     * Final aggregation function
     * @param partials
     * @return
     */
    public V finalAggregate(List<V> partials);
  }
  
  /*
   * MTSOperator sends window outputs to OutputHandler
   */
  @DefaultImplementation(DefaultOutputHandler.class)
  public interface OutputHandler<V> extends EventHandler<WindowOutput<V>>{
    
  }
  
  /*
   * Start of MTSOperator
   */
  public void start();
  
  /*
   * It receives input from this function
   * 
   * @param val input value
   */
  public void execute(final I val);
  
  /*
   * Add a new timescale. MTS have to produce new outputs which are related to this timescale. 
   */
  @Override
  public void onTimescaleAddition(final Timescale ts);
  
  /*
   * Remove an existing timescale. MTS have not to produce outputs which are related to this timescale.
   */
  @Override
  public void onTimescaleDeletion(final Timescale ts);
}
