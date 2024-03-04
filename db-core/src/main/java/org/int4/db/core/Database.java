package org.int4.db.core;

/**
 * Represents a database on which transactions can be started.
 */
public interface Database extends DatabaseFunctions<Transaction, DatabaseException> {
}
