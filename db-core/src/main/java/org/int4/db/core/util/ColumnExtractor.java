package org.int4.db.core.util;

/**
 * A function that extracts a given column from a type {@code T}.
 *
 * @param <T> the type being extracted from
 */
public interface ColumnExtractor<T> {

  /**
   * Extracts a given column from the given type {@code T}.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @param columnIndex a column index, cannot be negative
   * @return the extracted value, can be {@code null}
   */
  Object extract(T t, int columnIndex);

}
