package org.int4.db.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Represents a database on which transactions can be started.
 */
public final class Database {
  private final Supplier<Connection> connectionProvider;

  /**
   * Constructs a new instance.
   *
   * @param connectionProvider a supplier which provides {@link Connection}s, cannot be {@code null}
   * @throws NullPointerException when any arguments is {@code null}
   */
  public Database(Supplier<Connection> connectionProvider) {
    this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
  }

  /**
   * Starts a read/write transaction which throws the unchecked
   * {@link DatabaseException} if a database error occurs.
   *
   * @return a new {@link Transaction}, never {@code null}
   */
  public Transaction beginTransaction() {
    return beginTransaction(false);
  }

  /**
   * Starts a read only transaction which throws the unchecked
   * {@link DatabaseException} if a database error occurs.
   *
   * @return a new {@link Transaction}, never {@code null}
   */
  public Transaction beginReadOnlyTransaction() {
    return beginTransaction(true);
  }

  /**
   * Starts a read/write transaction.
   *
   * @return a new {@link Transaction}, never {@code null}
   * @throws SQLException when a database error occurs
   */
  public CheckedTransaction beginCheckedTransaction() throws SQLException {
    return beginCheckedTransaction(false);
  }

  /**
   * Starts a read only transaction.
   *
   * @return a new {@link Transaction}, never {@code null}
   * @throws SQLException when a database error occurs
   */
  public CheckedTransaction beginCheckedReadOnlyTransaction() throws SQLException {
    return beginCheckedTransaction(true);
  }

  private Transaction beginTransaction(boolean readOnly) {
    return new Transaction(connectionProvider, readOnly);
  }

  private CheckedTransaction beginCheckedTransaction(boolean readOnly) throws SQLException {
    return new CheckedTransaction(connectionProvider, readOnly);
  }
}
