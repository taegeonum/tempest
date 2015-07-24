package org.edu.snu.tempest.operators.dynamicmts.signal;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.edu.snu.tempest.operators.Timescale;
import org.edu.snu.tempest.operators.dynamicmts.TimescaleSignalListener;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkMTSParameters;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkSignalReceiver;
import org.edu.snu.tempest.operators.dynamicmts.signal.impl.ZkSignalSender;
import org.edu.snu.tempest.utils.Monitor;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ZkTimescaleSignalTest {

  private final String identifier = "test-signal";
  private final int port = 2181;
  private final String host = "localhost";

  /**
   * ZkSignalSender sends three timescales to Zookeeper.
   * ZKSignalReceiver receives the timescales. 
   */
  @Test
  public void addTimescaleSignalTest() throws Exception {
    final Monitor monitor = new Monitor();
    final ConcurrentLinkedQueue<Timescale> timescales = new ConcurrentLinkedQueue<>();
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalReceiver connector = injector.getInstance(MTSSignalReceiver.class);
    connector.addTimescaleSignalListener(new TestTimescaleListener(timescales, monitor, 3));
    connector.start();
    
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    final Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalSender client = injector2.getInstance(MTSSignalSender.class);

    final Timescale t1 = new Timescale(5, 1);
    final Timescale t2 = new Timescale(10, 2);
    final Timescale t3 = new Timescale(15, 3);

    client.addTimescale(t1);
    Thread.sleep(500);
    client.addTimescale(t2);
    Thread.sleep(500);
    client.addTimescale(t3);
    monitor.mwait();
    
    Assert.assertEquals(t1, timescales.poll());
    Assert.assertEquals(t2, timescales.poll());
    Assert.assertEquals(t3, timescales.poll());

    client.close();
    connector.close();
  }
  
  /**
   * Add two timescales and remove one timescale. 
   */
  @Test
  public void removeTimescaleSignalTest() throws Exception {
    final Monitor monitor = new Monitor();
    final ConcurrentLinkedQueue<Timescale> timescales = new ConcurrentLinkedQueue<>();
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalReceiver connector = injector.getInstance(MTSSignalReceiver.class);
    connector.addTimescaleSignalListener(new TestTimescaleListener(timescales, monitor, 3));
    connector.start();
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    final Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    final MTSSignalSender client = injector2.getInstance(MTSSignalSender.class);

    final Timescale t1 = new Timescale(5, 1);
    final Timescale t2 = new Timescale(10, 2);

    client.addTimescale(t1);
    Thread.sleep(500);
    client.addTimescale(t2);
    Thread.sleep(500);
    client.removeTimescale(t2);
    monitor.mwait();

    Assert.assertEquals(timescales.size(), 1);
    Assert.assertEquals(t1, timescales.poll());
    client.close();
    connector.close();

  }
  
  static class TestTimescaleListener implements TimescaleSignalListener {
    private final Collection<Timescale> list;
    private final Monitor monitor;
    private final AtomicInteger counter;
    
    public TestTimescaleListener(final Collection<Timescale> list,  final Monitor monitor, final int numOfTimescales) {
      this.list = list;
      this.monitor = monitor;
      this.counter = new AtomicInteger(numOfTimescales);
    }

    @Override
    public void onTimescaleAddition(final Timescale ts, final long startTime) {
      this.list.add(ts);
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }

    @Override
    public void onTimescaleDeletion(final Timescale ts) {
      this.list.remove(ts);
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }
  }
}
