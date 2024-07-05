package org.int4.db.core.fluent;

import java.util.Iterator;

import org.int4.db.core.reflect.Row;

/**
 * Bridge interface between the JDBC world and the templated world.
 *
 * <p>Note: The returned iterators make no guarantees as to the validity of
 * the returned {@link Row} instances after {@link Iterator#hasNext()} has
 * been called, and may reuse such instances. Rows that must be kept past
 * the next iteration should be copied.
 */
public interface SQLResult {

  /**
   * Creates a {@link Iterator} from an executed SQL statement.
   *
   * @return a {@link Iterator}, never {@code null}
   */
  Iterator<Row> createIterator();

  /**
   * Creates a {@link Iterator} on the generated keys resulting from an
   * executed SQL statement.
   *
   * @return a {@link Iterator}, never {@code null}
   */
  Iterator<Row> createGeneratedKeysIterator();

  /**
   * Returns the number of rows affected resulting from an executed SQL statement.
   *
   * @return a number of rows, never negative
   */
  long getUpdateCount();
}