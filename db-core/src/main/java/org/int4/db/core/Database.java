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
  private final RetryStrategy retryStrategy;

  Database(Supplier<Connection> connectionProvider, RetryStrategy retryStrategy) {
    this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
    this.retryStrategy = retryStrategy == null ? RetryStrategy.NONE : retryStrategy;
  }

  @Override
  public Transaction beginTransaction(boolean readOnly) {
    return new Transaction(connectionProvider, readOnly);
  }

  @Override
  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  @Override
  public SQLException unwrap(DatabaseException exception) {
    return exception.getSQLException();
  }

  @Override
  public Class<DatabaseException> exceptionType() {
    return DatabaseException.class;
  }
}
