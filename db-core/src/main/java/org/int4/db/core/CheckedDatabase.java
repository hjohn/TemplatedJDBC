package org.int4.db.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

import org.int4.db.core.CheckedTransaction.SQLExceptionWrapper;

/**
 * Represents a database on which transactions can be started.
 */
public final class CheckedDatabase implements DatabaseFunctions<CheckedTransaction, SQLException> {
  private final Supplier<Connection> connectionProvider;

  CheckedDatabase(Supplier<Connection> connectionProvider) {
    this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
  }

  @Override
  public CheckedTransaction beginTransaction(boolean readOnly) throws SQLException {
    return new CheckedTransaction(connectionProvider, readOnly);
  }

  @Override
  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  @Override
  public SQLException unwrap(SQLException exception) {
    return exception instanceof SQLExceptionWrapper w ? w.getSQLException() : exception;
  }

  @Override
  public Class<SQLException> exceptionType() {
    return SQLException.class;
  }
}
