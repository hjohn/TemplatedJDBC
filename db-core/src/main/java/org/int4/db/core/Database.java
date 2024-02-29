package org.int4.db.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Represents a database on which transactions can be started.
 */
public final class Database implements DatabaseFunctions<Transaction, DatabaseException> {
  private final Supplier<Connection> connectionProvider;

  /**
   * Constructs a new instance.
   *
   * @param connectionProvider a supplier which provides {@link Connection}s, cannot be {@code null}
   * @throws NullPointerException when any arguments is {@code null}
   */
  Database(Supplier<Connection> connectionProvider) {
    this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
  }

  @Override
  public Transaction beginTransaction(boolean readOnly) {
    return new Transaction(connectionProvider, readOnly);
  }
}
