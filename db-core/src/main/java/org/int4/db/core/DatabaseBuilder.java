package org.int4.db.core;

import java.sql.Connection;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Builder for {@link Database} instances.
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

  private DatabaseBuilder(Supplier<Connection> connectionSupplier) {
    this.connectionSupplier = connectionSupplier;
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
    return new Database(connectionSupplier);
  }
}
