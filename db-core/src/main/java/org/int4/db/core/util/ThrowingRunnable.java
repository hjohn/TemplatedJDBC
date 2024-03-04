package org.int4.db.core.util;

/**
 * A runnable that can throw a checked exception.
 *
 * @param <X> the exception type
 */
public interface ThrowingRunnable<X extends Throwable> {

  /**
   * Runs an operation.
   *
   * @throws X when an error occurred
   */
  void run() throws X;
}
