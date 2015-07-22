package org.edu.snu.tempest.operators.dynamicmts.signal.impl;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.remote.Decoder;
import org.apache.reef.wake.remote.Encoder;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignal;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignalDecoder;
import org.edu.snu.tempest.operators.dynamicmts.signal.TimescaleSignalEncoder;

public class ZkMTSParameters {
  
  @NamedParameter(doc = "zookeeper server address", short_name = "zkAddress", default_value="localhost:2000")
  public static final class ZkServerAddress implements Name<String> {}
  
  @NamedParameter(doc = "identifier", short_name = "zkIdentifier", default_value="default")
  public static final class OperatorIdentifier implements Name<String> {}
  
  @NamedParameter(doc = "Timescale encoder", default_class=TimescaleSignalEncoder.class)
  public static final class ZkTSEncoder implements Name<Encoder<TimescaleSignal>> {}
  
  @NamedParameter(doc = "Timescale decoder", default_class=TimescaleSignalDecoder.class)
  public static final class ZkTSDecoder implements Name<Decoder<TimescaleSignal>> {}
  
  @NamedParameter(doc = "Zookeeper namespace", short_name = "zkNamespace", default_value="mtss-signal") 
  public static final class ZkMTSNamespace implements Name<String> {}
  
  @NamedParameter(doc = "Zookeeper Client data sending period (ms)", default_value = "500") 
  public static final class ZkDataSendingPeriod implements Name<Long> {}
  
  @NamedParameter(doc = "Zookeeper retry times", default_value = "3")
  public static final class ZkRetryTimes implements Name<Integer> {}
  
  @NamedParameter(doc = "Zookeeper retry interval", default_value = "500")
  public static final class ZkRetryPeriod implements Name<Integer> {}
}
