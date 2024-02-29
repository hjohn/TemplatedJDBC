package org.int4.db.core;

import java.sql.Connection;
import java.util.Objects;
import java.util.function.Supplier;

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
    return new Database(connectionSupplier, retryStrategy);
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
    return new CheckedDatabase(connectionSupplier, retryStrategy);
  }
}
