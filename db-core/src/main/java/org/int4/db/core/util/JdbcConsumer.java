package org.int4.db.core.util;

import java.sql.SQLException;

/**
 * A {@link ThrowingConsumer} that is configured to throw {@link SQLException}.
 *
 * @param <T> the type to consume
 */
public interface JdbcConsumer<T> extends ThrowingConsumer<T, SQLException> {
}
