package org.int4.db.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

public class Database {
  private final Supplier<Connection> connectionProvider;

  public Database(Supplier<Connection> connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  public Transaction beginTransaction() {
    return beginTransaction(false);
  }

  public Transaction beginReadOnlyTransaction() {
    return beginTransaction(true);
  }

  public CheckedTransaction beginCheckedTransaction() throws SQLException {
    return beginCheckedTransaction(false);
  }

  public CheckedTransaction beginCheckedReadOnlyTransaction() throws SQLException {
    return beginCheckedTransaction(true);
  }

  private Transaction beginTransaction(boolean readOnly) {
    return new Transaction(connectionProvider, readOnly);
  }

  private CheckedTransaction beginCheckedTransaction(boolean readOnly) throws SQLException {
    return new CheckedTransaction(connectionProvider, readOnly);
  }
}
