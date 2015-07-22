package org.edu.snu.tempest.operators.staticmts;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.edu.snu.tempest.operators.common.RelationCube;
import org.edu.snu.tempest.operators.staticmts.impl.StaticRelationGraphImpl;

/**
 * Static RelationGraph interface.
 */
@DefaultImplementation(StaticRelationGraphImpl.class)
public interface StaticRelationGraph<T> extends RelationCube<T> {

  /**
   * Return next slice time for SlicedWindowOperator.
   * @return
   */
  long nextSliceTime();
}
