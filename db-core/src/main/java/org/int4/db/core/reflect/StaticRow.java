package org.int4.db.core.reflect;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * An implementation of {@link Row} that gets its data from
 * an array.
 */
class StaticRow implements Row {
  private final Object[] data;

  /**
   * Constructs a new instance.
   *
   * <p>Note: the given array is not copied and should not
   * be mutated after calling this constructor.
   *
   * @param data an array, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  StaticRow(Object... data) {
    this.data = Objects.requireNonNull(data, "data");
  }

  @Override
  public int getColumnCount() {
    return data.length;
  }

  private void ensureValidColumnIndex(int columnIndex) {
    if(columnIndex < 0 || columnIndex >= data.length) {
      throw new IllegalArgumentException("columnIndex is out of range, must be between 0 and " + (data.length - 1) + ", but was: " + columnIndex);
    }
  }

  @Override
  public String getString(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return (String)data[columnIndex];
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) {
    ensureValidColumnIndex(columnIndex);

    Object value = getObject(columnIndex);

    if(value instanceof Timestamp t && type == Instant.class) {
      return type.cast(t.toInstant());
    }

    return type.cast(value);
  }

  @Override
  public Object getObject(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return data[columnIndex];
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return (Boolean)data[columnIndex];
  }

  @Override
  public int getInt(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return (Integer)data[columnIndex];
  }

  @Override
  public long getLong(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return (Long)data[columnIndex];
  }

  @Override
  public double getDouble(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return (Double)data[columnIndex];
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    ensureValidColumnIndex(columnIndex);

    return (byte[])data[columnIndex];
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(data);
  }

  @Override
  public boolean equals(Object obj) {
    if(this == obj) {
      return true;
    }
    if(obj == null || getClass() != obj.getClass()) {
      return false;
    }

    StaticRow other = (StaticRow)obj;

    return Arrays.deepEquals(data, other.data);
  }

  @Override
  public String toString() {
    return "Row[data = " + Arrays.toString(data) + "]";
  }
}
