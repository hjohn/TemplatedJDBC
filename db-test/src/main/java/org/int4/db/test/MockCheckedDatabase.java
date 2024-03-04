package org.int4.db.test;

import java.sql.SQLException;

import org.int4.db.core.CheckedDatabase;
import org.int4.db.core.CheckedTransaction;

/**
 * A database which can return mocked responses when SQL statements match a
 * regular expression.
 */
public class MockCheckedDatabase extends AbstractMockDatabase<SQLException> implements CheckedDatabase {

  @Override
  public CheckedTransaction beginTransaction(boolean readOnly) {
    return new CheckedTransaction(() -> null, readOnly, (tx, sql) -> new MockContext(sql));
  }

  @Override
  public SQLException unwrap(SQLException exception) {
    return exception;
  }

  @Override
  public Class<SQLException> exceptionType() {
    return SQLException.class;
  }
}
