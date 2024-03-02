package org.int4.db.core.util;

/**
 * A supplier that can throw a checked exception.
 *
 * @param <R> the type supplied
 * @param <X> the exception type
 */
public interface ThrowingSupplier<R, X extends Throwable> {

  /**
   * Gets a value from this supplier.
   *
   * @return a value, can be {@code null}
   * @throws X when an error occurs
   */
  R get() throws X;
}
