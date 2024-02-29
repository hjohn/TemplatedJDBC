package org.int4.db.core;

import java.sql.SQLException;
import java.util.Objects;

import org.int4.db.core.util.ThrowingConsumer;
import org.int4.db.core.util.ThrowingFunction;

interface DatabaseFunctions<T extends BaseTransaction<X>, X extends Exception> {

  /**
   * Starts a read/write transaction which throws the unchecked
   * {@link DatabaseException} if a database error occurs.
   *
   * @return a new {@link Transaction}, never {@code null}
   * @throws X when a database exception occurs
   */
  default T beginTransaction() throws X {
    return beginTransaction(false);
  }

  /**
   * Starts a read only transaction which throws the unchecked
   * {@link DatabaseException} if a database error occurs.
   *
   * @return a new {@link Transaction}, never {@code null}
   * @throws X when a database exception occurs
   */
  default T beginReadOnlyTransaction() throws X {
    return beginTransaction(true);
  }

  /**
   * Performs a read only operation on the database, and returns
   * its result. The operation will be retried according to the configured
   * {@link RetryStrategy}.
   *
   * @param <R> the result type
   * @param operation a read only operation, cannot be {@code null}
   * @return the result of the operation, can be {@code null}
   * @throws X when a database exception occurs
   * @throws NullPointerException when any argument is {@code null}
   */
  default <R> R query(ThrowingFunction<T, R, X> operation) throws X {
    return apply(Objects.requireNonNull(operation, "operation"), true);
  }

  /**
   * Accepts a modifying operation on the database which produces no
   * result. The operation will be retried according to the configured
   * {@link RetryStrategy}.
   *
   * @param operation a modifying operation, cannot be {@code null}
   * @throws X when a database exception occurs
   * @throws NullPointerException when any argument is {@code null}
   */
  default void accept(ThrowingConsumer<T, X> operation) throws X {
    apply(toFunction(Objects.requireNonNull(operation, "operation")), false);
  }

  /**
   * Applies a modifying operation on the database, and returns
   * its result. The operation will be retried according to the configured
   * {@link RetryStrategy}.
   *
   * @param <R> the result type
   * @param operation a modifying operation, cannot be {@code null}
   * @return the result of the operation, can be {@code null}
   * @throws X when a database exception occurs
   * @throws NullPointerException when any argument is {@code null}
   */
  default <R> R apply(ThrowingFunction<T, R, X> operation) throws X {
    return apply(Objects.requireNonNull(operation, "operation"), false);
  }

  /**
   * Applies an operation on the database, and returns its result.
   * The operation will be retried according to the configured
   * {@link RetryStrategy}.
   *
   * @param <R> the result type
   * @param operation an operation, cannot be {@code null}
   * @param readOnly whether the operation should use a read only transaction
   * @return the result of the operation, can be {@code null}
   * @throws X when a database exception occurs
   * @throws NullPointerException when any argument is {@code null}
   */
  default <R> R apply(ThrowingFunction<T, R, X> operation, boolean readOnly) throws X {
    Objects.requireNonNull(operation, "operation");

    for(int failCount = 1; ; failCount++) {
      try(T tx = beginTransaction(readOnly)) {
        R result = operation.apply(tx);

        tx.commit();

        return result;
      }
      catch(Exception e) {
        if(exceptionType().isInstance(e)) {
          @SuppressWarnings("unchecked")
          X x = (X)e;

          if(retryStrategy().retry(failCount, unwrap(x))) {
            continue;
          }
        }

        throw e;
      }
    }
  }

  /**
   * Begins a transaction. Transactions must be closed after use.
   *
   * @param readOnly whether a read only transaction should be created
   * @return a transaction, never {@code null}
   * @throws X when a database exception occurs
   */
  T beginTransaction(boolean readOnly) throws X;

  /**
   * Returns the {@link RetryStrategy} in use by this database.
   *
   * @return the {@link RetryStrategy} in use by this database, never {@code null}
   */
  RetryStrategy retryStrategy();

  /**
   * Unwraps an exception to get at the root cause. This is always an
   * {@link SQLException}.
   *
   * <p>If the given exception is already an {@link SQLException}, but not
   * the root cause, then this function must unwrap it to return the root
   * cause.
   *
   * @param exception the exception to unwrap, cannot be {@code null}
   * @return an {@link SQLException}, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  SQLException unwrap(X exception);

  /**
   * Returns the type of exception this database throws.
   *
   * @return an exception type, never {@code null}
   */
  Class<X> exceptionType();

  private static <T, X extends Exception> ThrowingFunction<T, Void, X> toFunction(ThrowingConsumer<T, X> consumer) {
    return t -> {
      consumer.accept(t);
      return null;
    };
  }
}
