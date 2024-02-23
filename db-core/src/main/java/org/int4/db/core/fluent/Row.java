package org.int4.db.core.fluent;

import java.sql.SQLException;

public interface Row {

  byte[] getBytes(int columnIndex) throws SQLException;
  double getDouble(int columnIndex) throws SQLException;
  long getLong(int columnIndex) throws SQLException;
  int getInt(int columnIndex) throws SQLException;
  boolean getBoolean(int columnIndex) throws SQLException;
  <T> T getObject(int columnIndex, Class<T> type) throws SQLException;
  Object getObject(int columnIndex) throws SQLException;
  String getString(int columnIndex) throws SQLException;
  int getColumnCount() throws SQLException;

}
