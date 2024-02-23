package org.int4.db.core;

public class DatabaseException extends RuntimeException {

  public DatabaseException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatabaseException(String message) {
    super(message);
  }
}
