package vldb.operator.window.timescale.cutty;

import vldb.operator.window.timescale.common.Timespan;

/**
 * Created by taegeonum on 8/25/17.
 */
public interface Fat<V> {

  public void append(Timespan timespan, V partial);

  public void removeUpTo(long time);

  public V merge(long from);

  public void dump();
}
