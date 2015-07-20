package org.edu.snu.tempest.operator.relationcube;

import org.edu.snu.tempest.Timescale;

/**
 * RelationCube interface.
 * It saves partial aggregation
 * and produces final aggregation by aggregating partial aggregation.
 */
public interface RelationCube<T> {
  /**
   * Save partial output to RelationCube.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  void savePartialOutput(long startTime, long endTime, T output);

  /**
   * Produce final aggregation of [startTime, endTime] window output.
   *
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   */
  T finalAggregate(long startTime, long endTime, Timescale ts);

  /**
   * Add timescales to RelationCube.
   * @param ts timescale
   * @param time time to addition.
   */
  void addTimescale(Timescale ts, long time);

  /**
   * Remove timescales from RelationCube.
   * @param ts timescale
   * @param time time to deletion
   */
  void removeTimescale(final Timescale ts, long time);
}
