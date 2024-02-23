package org.int4.db.core.fluent;

import java.sql.SQLException;

/**
 * Represents a single row of a result set.
 */
public interface Row {

  /**
   * Gets the number of columns in this row.
   *
   * @return the number of columns in this row, always positive
   * @throws SQLException when a database error occurs
   */
  int getColumnCount() throws SQLException;

  /**
   * Gets the value of the indicated column as a byte array.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value as a byte array, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  byte[] getBytes(int columnIndex) throws SQLException;

  /**
   * Gets the value of the indicated column as a {@link String}.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value as a {@link String}, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  String getString(int columnIndex) throws SQLException;

  /**
   * Gets the value of the indicated column without further conversion.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  Object getObject(int columnIndex) throws SQLException;

  /**
   * Gets the value of the indicated column as the indicated type {@code T}.
   * The first column has index 0.
   *
   * @param <T> the preferred type
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @param type a type to convert to, cannot be {@code null}
   * @return the converted value, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  <T> T getObject(int columnIndex, Class<T> type) throws SQLException;

  /**
   * Gets the value of the indicated column as a {@code double}.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value as a {@code double}, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  double getDouble(int columnIndex) throws SQLException;

  /**
   * Gets the value of the indicated column as a {@code long}.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value as a {@code long}, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  long getLong(int columnIndex) throws SQLException;

  /**
   * Gets the value of the indicated column as a {@code int}.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value as a {@code int}, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  int getInt(int columnIndex) throws SQLException;

  /**
   * Gets the value of the indicated column as a {@code boolean}.
   * The first column has index 0.
   *
   * @param columnIndex a column index, cannot be negative and must be less than the column count
   * @return the value as a {@code boolean}, never {@code null}
   * @throws SQLException when the column index is invalid, or the value could not be
   *   converted to the requested type
   */
  boolean getBoolean(int columnIndex) throws SQLException;

}
