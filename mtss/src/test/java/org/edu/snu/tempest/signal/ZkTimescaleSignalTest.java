package org.edu.snu.tempest.signal;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.edu.snu.tempest.Timescale;
import org.edu.snu.tempest.signal.impl.ZkMTSParameters;
import org.edu.snu.tempest.signal.impl.ZkSignalReceiver;
import org.edu.snu.tempest.signal.impl.ZkSignalSender;
import org.edu.snu.tempest.utils.Monitor;
import org.junit.Assert;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ZkTimescaleSignalTest {

  private String identifier = "test-signal";
  private String namespace = "test-mtss";
  private final int port = 2181;
  private final String host = "localhost";

  /*
   * ZkSignalSender sends three timescales to Zookeeper 
   * ZKSignalReceiver receives the timescales. 
   */
  //@Test
  public void addTimescaleSignalTest() throws Exception {

    Monitor monitor = new Monitor();
    ConcurrentLinkedQueue<Timescale> timescales = new ConcurrentLinkedQueue<>();
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkMTSNamespace.class, namespace);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    
    Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    injector.bindVolatileInstance(TimescaleSignalListener.class, new TestTimescaleListener(timescales, monitor, 3));
    MTSSignalReceiver connector = injector.getInstance(MTSSignalReceiver.class);
    
    connector.start();
    
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    MTSSignalSender client = injector2.getInstance(MTSSignalSender.class);
    
    Timescale t1 = new Timescale(5, 1);
    Timescale t2 = new Timescale(10, 2);
    Timescale t3 = new Timescale(15, 3);

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
  
  /*
   * Add two timescales and remove one timescale. 
   */
  
  //@Test
  public void removeTimescaleSignalTest() throws Exception {

    Monitor monitor = new Monitor();
    ConcurrentLinkedQueue<Timescale> timescales = new ConcurrentLinkedQueue<>();
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(ZkMTSParameters.OperatorIdentifier.class, identifier);
    cb.bindNamedParameter(ZkMTSParameters.ZkMTSNamespace.class, namespace);
    cb.bindNamedParameter(ZkMTSParameters.ZkServerAddress.class, host + ":" + port);
    cb.bindImplementation(MTSSignalReceiver.class, ZkSignalReceiver.class);
    
    Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    injector.bindVolatileInstance(TimescaleSignalListener.class, new TestTimescaleListener(timescales, monitor, 3));
    MTSSignalReceiver connector = injector.getInstance(MTSSignalReceiver.class);
    
    connector.start();
    
    cb.bindImplementation(MTSSignalSender.class, ZkSignalSender.class);
    Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    MTSSignalSender client = injector2.getInstance(MTSSignalSender.class);
    
    Timescale t1 = new Timescale(5, 1);
    Timescale t2 = new Timescale(10, 2);

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
    public void onTimescaleAddition(Timescale ts, final long startTime) {
      this.list.add(ts);
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }

    @Override
    public void onTimescaleDeletion(Timescale ts) {
      this.list.remove(ts);
      if (this.counter.decrementAndGet() == 0) {
        monitor.mnotify();
      }
    }
  }
}
