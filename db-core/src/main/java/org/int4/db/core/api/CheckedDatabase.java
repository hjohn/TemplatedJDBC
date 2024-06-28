package org.int4.db.core.api;

import java.sql.SQLException;

/**
 * Represents a database on which transactions can be started.
 */
public interface CheckedDatabase extends DatabaseFunctions<CheckedTransaction, SQLException> {
}
