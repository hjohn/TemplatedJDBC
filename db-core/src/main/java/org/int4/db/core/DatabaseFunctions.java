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
   * Begins a transaction. Transactions must be closed after use.
   *
   * @param readOnly whether a read only transaction should be created
   * @return a transaction, never {@code null}
   * @throws X when a database exception occurs
   */
  T beginTransaction(boolean readOnly) throws X;
}
