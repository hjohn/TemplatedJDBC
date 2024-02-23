package org.int4.db.core.fluent;

import org.int4.db.core.util.JdbcFunction;

public interface MappingSteps<T, X extends Exception> {

  /**
   * Assumes a statement which returns a result set.
   *
   * @param <R> result type of mapping function
   * @param mapper a mapping function, cannot be {@code null}
   * @return a new {@link ExecutionStep}, never {@code null}
   */
  <R> ExecutionStep<R, X> map(JdbcFunction<T, R> mapper);
}
