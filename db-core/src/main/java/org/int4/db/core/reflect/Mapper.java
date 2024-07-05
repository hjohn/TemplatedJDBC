package org.int4.db.core.reflect;

import java.util.function.Function;

/**
 * Converts {@link Row}s to a new type {@code T}
 *
 * @param <T> the type converted to
 */
public interface Mapper<T> extends Function<Row, T> {
}
