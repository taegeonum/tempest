package org.edu.snu.tempest.operators.common;

import org.edu.snu.tempest.operators.Timescale;

/**
 * RelationCube interface.
 *
 * It saves partial aggregation and produces final aggregation.
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
}
