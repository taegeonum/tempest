package org.edu.snu.tempest.utils;


import org.edu.snu.tempest.operators.dynamicmts.TimescaleSignalListener;
import org.edu.snu.tempest.operators.dynamicmts.signal.MTSSignalReceiver;

public final class TestSignalReceiver implements MTSSignalReceiver {
  private TimescaleSignalListener listener;

  @Override
  public void start() throws Exception {

  }

  @Override
  public void addTimescaleSignalListener(final TimescaleSignalListener listener) {
    this.listener = listener;
  }

  @Override
  public void close() throws Exception {

  }
}