package org.int4.db.test;

import java.sql.SQLException;

import org.int4.db.core.Database;
import org.int4.db.core.DatabaseException;
import org.int4.db.core.Transaction;

/**
 * A database which can return mocked responses when SQL statements match a
 * regular expression.
 */
public class MockDatabase extends AbstractMockDatabase<DatabaseException> implements Database {

  @Override
  public Transaction beginTransaction(boolean readOnly) throws DatabaseException {
    return new Transaction(() -> null, readOnly, (tx, sql) -> new MockContext(sql));
  }

  @Override
  public SQLException unwrap(DatabaseException exception) {
    return exception.getSQLException();
  }

  @Override
  public Class<DatabaseException> exceptionType() {
    return DatabaseException.class;
  }
}
