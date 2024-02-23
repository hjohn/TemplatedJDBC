package org.int4.db.core.fluent;

import org.int4.db.core.util.JdbcFunction;

/**
 * Provides steps to convert a type {@code T} to a new type of result.
 *
 * @param <T> the current result type
 * @param <X> the type of exception that can be thrown
 */
public interface MappingSteps<T, X extends Exception> {

  /**
   * Maps a result of type {@code T} to a new type {@code R}.
   *
   * @param <R> result type of mapping function
   * @param mapper a mapping function, cannot be {@code null}
   * @return a new {@link ExecutionStep}, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  <R> ExecutionStep<R, X> map(JdbcFunction<T, R> mapper);
}
