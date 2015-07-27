package edu.snu.tempest.operators.staticmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import edu.snu.tempest.operators.common.RelationCube;
import edu.snu.tempest.operators.staticmts.impl.StaticRelationGraphImpl;

/**
 * Static RelationGraph interface.
 */
@DefaultImplementation(StaticRelationGraphImpl.class)
public interface StaticRelationGraph<T> extends RelationCube<T> {

  /**
   * Return next slice time for SlicedWindowOperator.
   * @return next slice time
   */
  long nextSliceTime();
}
