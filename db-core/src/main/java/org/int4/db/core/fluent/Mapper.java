package org.int4.db.core.fluent;

import org.int4.db.core.util.JdbcFunction;

public interface Mapper<T> extends JdbcFunction<Row, T> {
}
