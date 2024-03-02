package org.int4.db.core.util;

import java.sql.SQLException;

/**
 * An iterator suitable to iterate over JDBC result sets.
 *
 * @param <E> the result type
 */
public interface JdbcIterator<E> {

  /**
   * Moves to the next element.
   *
   * @return {@code true} if there was a next element, {@code false} if
   *   the previous element was the last element
   * @throws SQLException when an error occurs
   */
  boolean next() throws SQLException;

  /**
   * Returns the current result.
   *
   * @return the current result, never {@code null}
   * @throws SQLException when an error occurs
   */
  E get() throws SQLException;
}
