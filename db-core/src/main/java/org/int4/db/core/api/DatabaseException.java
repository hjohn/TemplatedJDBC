package org.int4.db.core.api;

import java.sql.SQLException;
import java.util.Objects;

/**
 * Thrown when an error occurred while accessing the database.
 */
public class DatabaseException extends RuntimeException {

  /**
   * Constructs a new instance.
   *
   * @param message a message, cannot be {@code null}
   * @param cause an {@link SQLException}, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public DatabaseException(String message, SQLException cause) {
    super(Objects.requireNonNull(message, "message"), Objects.requireNonNull(cause, "cause"));
  }

  /**
   * Returns the {@link SQLException} wrapped by this exception.
   *
   * @return an {@link SQLException}, never {@code null}
   */
  public SQLException getSQLException() {
    return (SQLException)getCause();
  }
}
