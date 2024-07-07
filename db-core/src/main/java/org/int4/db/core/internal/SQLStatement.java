package org.int4.db.core.internal;

import java.sql.SQLException;

import org.int4.db.core.internal.bridge.SQLResult;

public interface SQLStatement extends AutoCloseable {
  SQLResult execute() throws SQLException;

  @Override
  void close() throws SQLException;
}