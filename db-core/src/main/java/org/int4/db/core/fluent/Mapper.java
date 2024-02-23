package org.int4.db.core.fluent;

import org.int4.db.core.util.JdbcFunction;

/**
 * Converts {@link Row}s to a new type {@code T}
 *
 * @param <T> the type converted to
 */
public interface Mapper<T> extends JdbcFunction<Row, T> {
}
