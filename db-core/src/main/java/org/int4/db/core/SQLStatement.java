package org.int4.db.core;

import java.sql.SQLException;

import org.int4.db.core.fluent.SQLResult;

interface SQLStatement extends AutoCloseable {
  SQLResult execute() throws SQLException;

  @Override
  void close() throws SQLException;
}