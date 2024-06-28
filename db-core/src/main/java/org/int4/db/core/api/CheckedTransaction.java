package org.int4.db.core.api;

import java.sql.SQLException;

/**
 * Represents a database transaction.
 */
public interface CheckedTransaction extends TransactionFunctions<SQLException> {
}
