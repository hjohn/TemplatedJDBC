package org.int4.db.core;

/**
 * Thrown when an error occurred while accessing the database.
 */
public class DatabaseException extends RuntimeException {

  DatabaseException(String message, Throwable cause) {
    super(message, cause);
  }

  DatabaseException(String message) {
    super(message);
  }
}
