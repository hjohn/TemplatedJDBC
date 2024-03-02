package org.int4.db.core.util;

/**
 * A consumer that can throw a checked exception.
 *
 * @param <T> the type to consume
 * @param <X> the exception type
 */
public interface ThrowingConsumer<T, X extends Throwable> {

  /**
   * Performs this operation on the given argument.
   *
   * @param t the input argument
   * @throws X when an error occurs
   */
  void accept(T t) throws X;
}
