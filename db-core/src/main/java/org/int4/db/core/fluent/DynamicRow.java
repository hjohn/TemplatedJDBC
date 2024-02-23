package org.int4.db.core.fluent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

public class DynamicRow implements Row {
  private final ResultSet rs;

  /*
   * Notes:
   * - Getting column count is often cheap (no database call), at least for Postgres
   * - Accessing columns by name is much slower and error prone (column names are not required to be unique in a result set)
   */

  DynamicRow(ResultSet rs) {
    this.rs = rs;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return rs.getMetaData().getColumnCount();  // at least for Postgres, this is very cheap (no database call)
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return rs.getString(columnIndex + 1);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    if(type == Instant.class) {
      return type.cast(rs.getObject(columnIndex + 1, Timestamp.class).toInstant());
    }

    return type.cast(rs.getObject(columnIndex + 1, type));
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return rs.getObject(columnIndex + 1);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return rs.getBoolean(columnIndex + 1);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return rs.getInt(columnIndex + 1);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return rs.getLong(columnIndex + 1);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return rs.getDouble(columnIndex + 1);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return rs.getBytes(columnIndex + 1);
  }
}
