package edu.snu.tempest.operators.common;

import edu.snu.tempest.operators.Timescale;

/**
 * RelationCube interface.
 *
 * It saves partial aggregation and produces final aggregation.
 */
public interface RelationCube<T> {
  /**
   * Save a partial output containing data starting from the startTime to endTime.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param output output
   */
  void savePartialOutput(long startTime, long endTime, T output);

  /**
   * Produce an output which is produced by final aggregation.
   * @param startTime start time of the output
   * @param endTime end time of the output
   * @param ts timescale
   * @return an aggregated output ranging from startTime to endTime.
   */
  T finalAggregate(long startTime, long endTime, Timescale ts);
}
