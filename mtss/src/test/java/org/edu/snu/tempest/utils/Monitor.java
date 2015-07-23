package org.edu.snu.tempest.utils;

import java.util.concurrent.atomic.AtomicBoolean;

public final class Monitor {
  private final AtomicBoolean finished = new AtomicBoolean(false);

  public void mwait() throws InterruptedException {
    synchronized (this) {
      while (!finished.get()) {
        this.wait();
      }
      finished.compareAndSet(true, false);
    }
  }

  public void mnotify() {
    synchronized (this) {
      finished.compareAndSet(false, true);
      this.notifyAll();
    }
  }
}
