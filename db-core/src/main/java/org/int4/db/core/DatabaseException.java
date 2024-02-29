package org.int4.db.core;

import java.sql.SQLException;
import java.util.Objects;

/**
 * Thrown when an error occurred while accessing the database.
 */
public class DatabaseException extends RuntimeException {

  DatabaseException(String message, SQLException cause) {
    super(message, Objects.requireNonNull(cause, "cause"));
  }

  SQLException getSQLException() {
    return (SQLException)getCause();
  }
}
