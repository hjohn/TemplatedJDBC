package org.int4.db.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

import org.int4.db.core.CheckedTransaction.SQLExceptionWrapper;

/**
 * Builder for {@link Database} and {@link CheckedDatabase} instances.
 */
public class DatabaseBuilder {

  /**
   * Creates a new builder given a connection supplier.
   *
   * @param connectionSupplier a connection supplier, cannot be {@code null}
   * @return a new builder, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static DatabaseBuilder using(Supplier<Connection> connectionSupplier) {
    return new DatabaseBuilder(Objects.requireNonNull(connectionSupplier, "connectionSupplier"));
  }

  private final Supplier<Connection> connectionSupplier;

  private RetryStrategy retryStrategy = RetryStrategy.NONE;

  private DatabaseBuilder(Supplier<Connection> connectionSupplier) {
    this.connectionSupplier = connectionSupplier;
  }

  /**
   * Sets the retry strategy for the object produced by this builder.
   *
   * @param retryStrategy a retry strategy to use, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   * @return this
   */
  public DatabaseBuilder withRetryStrategy(RetryStrategy retryStrategy) {
    this.retryStrategy = Objects.requireNonNull(retryStrategy, "retryStrategy");

    return this;
  }

  /**
   * Builds a {@link Database} instance using this builder's configuration.
   *
   * <p>All operations on this instance will throw the unchecked
   * {@link DatabaseException} when a database error occurs.
   *
   * @return a {@link Database} instance, never {@code null}
   */
  public Database build() {
    return new DefaultDatabase(connectionSupplier, retryStrategy);
  }

  /**
   * Builds a {@link CheckedDatabase} instance using this builder's configuration.
   *
   * <p>All operations on this instance will throw the checked
   * {@link java.sql.SQLException} when a database error occurs.
   *
   * @return a {@link CheckedDatabase} instance, never {@code null}
   */
  public CheckedDatabase throwingSQLExceptions() {
    return new DefaultCheckedDatabase(connectionSupplier, retryStrategy);
  }

  private static class DefaultDatabase implements Database {
    private final Supplier<Connection> connectionSupplier;
    private final RetryStrategy retryStrategy;

    DefaultDatabase(Supplier<Connection> connectionSupplier, RetryStrategy retryStrategy) {
      this.connectionSupplier = connectionSupplier;
      this.retryStrategy = retryStrategy;
    }

    @Override
    public Transaction beginTransaction(boolean readOnly) {
      return new Transaction(connectionSupplier, readOnly);
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

  private static class DefaultCheckedDatabase implements CheckedDatabase {
    private final Supplier<Connection> connectionSupplier;
    private final RetryStrategy retryStrategy;

    DefaultCheckedDatabase(Supplier<Connection> connectionSupplier, RetryStrategy retryStrategy) {
      this.connectionSupplier = connectionSupplier;
      this.retryStrategy = retryStrategy;
    }

    @Override
    public CheckedTransaction beginTransaction(boolean readOnly) {
      return new CheckedTransaction(connectionSupplier, readOnly);
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
}
