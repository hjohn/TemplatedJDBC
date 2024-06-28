package org.int4.db.core.internal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import org.int4.db.core.fluent.Row;
import org.int4.db.core.fluent.RowAccessException;

class DynamicRow implements Row {
  private final ResultSet rs;

  /*
   * Notes:
   * - Getting column count is often cheap (no database call), at least for Postgres
   * - Accessing columns by name is much slower and error prone (column names are not required to be unique in a result set)
   *
   * This class tunnels the SQLException in a wrapper as it is only supplied in cases
   * when data is being consumed directly as part of a running transaction. In all
   * other cases, a static row is returned which can't throw any exceptions.
   */

  DynamicRow(ResultSet rs) {
    this.rs = rs;
  }

  @Override
  public int getColumnCount() {
    try {
      return rs.getMetaData().getColumnCount();  // at least for Postgres, this is very cheap (no database call)
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public String getString(int columnIndex) {
    try {
      return rs.getString(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) {
    try {
      if(type == Instant.class) {
        return type.cast(rs.getObject(columnIndex + 1, Timestamp.class).toInstant());
      }

      return type.cast(rs.getObject(columnIndex + 1, type));
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public Object getObject(int columnIndex) {
    try {
      return rs.getObject(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    try {
     return rs.getBoolean(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public int getInt(int columnIndex) {
    try {
      return rs.getInt(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public long getLong(int columnIndex) {
    try {
      return rs.getLong(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public double getDouble(int columnIndex) {
    try {
      return rs.getDouble(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    try {
      return rs.getBytes(columnIndex + 1);
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }
}
