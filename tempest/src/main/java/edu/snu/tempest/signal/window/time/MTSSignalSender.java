/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.tempest.signal.window.time;

import edu.snu.tempest.signal.SignalSenderStage;
import edu.snu.tempest.signal.impl.ZkMTSParameters;
import edu.snu.tempest.signal.impl.ZkSignalSenderStage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Zookeeper client for sending timescale to MTSWindowOperator.
 * It parses command line arguments and sends timescale signal to ZkMTSSignalReceiver.
 */
public final class MTSSignalSender {
  
  @NamedParameter(doc = "timescale window size(sec)", short_name = "w")
  public static final class WindowSize implements Name<Integer> {}
  
  @NamedParameter(doc = "timescale interval (sec)", short_name = "i") 
  public static final class Interval implements Name<Integer> {}
  
  @NamedParameter(doc = "type of signal (addition/deletion)", short_name = "type")
  public static final class TypeOfSignal implements Name<String> {}

  @NamedParameter(doc = "identifier", short_name = "mts_identifier", default_value="default")
  public static final class OperatorIdentifier implements Name<String> {}

  private MTSSignalSender() {
  }

  private static Configuration getCommandLineConf(final String[] args) throws BindException, IOException {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    final CommandLine cl = new CommandLine(cb)
        .registerShortNameOfClass(OperatorIdentifier.class)
        .registerShortNameOfClass(ZkMTSParameters.ZkServerAddress.class)
        .registerShortNameOfClass(WindowSize.class)
        .registerShortNameOfClass(Interval.class)
        .registerShortNameOfClass(TypeOfSignal.class)
        .processCommandLine(args);

    return cl.getBuilder().build();
  }
  
  public static void main(final String[] args) throws Exception {
    final Configuration commandConf = getCommandLineConf(args);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(SignalSenderStage.class, ZkSignalSenderStage.class);
    cb.addConfiguration(commandConf);

    final Injector ij = Tang.Factory.getTang().newInjector(cb.build());
    final SignalSenderStage<TimescaleSignal> sender = ij.getInstance(SignalSenderStage.class);
    final String type = ij.getNamedInstance(TypeOfSignal.class);
    final String identifier = ij.getNamedInstance(OperatorIdentifier.class);
    final long windowSize = ij.getNamedInstance(WindowSize.class);
    final long interval = ij.getNamedInstance(Interval.class);
    final long startTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());

    if (type.equalsIgnoreCase("addition")) {
      sender.sendSignal(identifier, new TimescaleSignal(windowSize, interval, TimescaleSignal.ADDITION, startTime));
    } else if (type.equalsIgnoreCase("deletion")) {
      sender.sendSignal(identifier, new TimescaleSignal(windowSize, interval, TimescaleSignal.DELETION, startTime));
    } else {
      throw new RuntimeException("Invalid signal type: " + type);
    }
    sender.close();
  }
}
