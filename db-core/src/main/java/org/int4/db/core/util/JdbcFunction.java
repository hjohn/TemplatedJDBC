package org.int4.db.core.util;

import java.sql.SQLException;

public interface JdbcFunction<T, R> extends ThrowingFunction<T, R, SQLException> {
}
