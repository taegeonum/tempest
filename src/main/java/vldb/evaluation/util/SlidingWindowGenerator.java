package vldb.evaluation.util;

import vldb.operator.window.timescale.Timescale;

import java.util.List;

public interface SlidingWindowGenerator {

  List<Timescale> generateSlidingWindows(final int num);
}
