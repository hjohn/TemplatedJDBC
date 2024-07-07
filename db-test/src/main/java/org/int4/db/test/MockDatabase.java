package org.int4.db.test;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;

import org.int4.db.core.api.Database;
import org.int4.db.core.api.DatabaseException;
import org.int4.db.core.api.Transaction;
import org.int4.db.core.api.TransactionResult;
import org.int4.db.core.fluent.StatementNode;
import org.int4.db.core.internal.SafeSQL;

/**
 * A database which can return mocked responses when SQL statements match a
 * regular expression.
 */
public class MockDatabase extends AbstractMockDatabase<DatabaseException> implements Database {

  @Override
  public Transaction beginTransaction(boolean readOnly) throws DatabaseException {
    return new InternalTransaction();
  }

  @Override
  public SQLException unwrap(DatabaseException exception) {
    return exception.getSQLException();
  }

  @Override
  public Class<DatabaseException> exceptionType() {
    return DatabaseException.class;
  }

  class InternalTransaction implements Transaction {

    @Override
    public void commit() throws DatabaseException {
    }

    @Override
    public void rollback() throws DatabaseException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addCompletionHook(Consumer<TransactionResult> consumer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws DatabaseException {
    }

    @Override
    public StatementNode<DatabaseException> process(StringTemplate stringTemplate) throws DatabaseException {
      return new StatementNode<>(createContext(new SafeSQL(stringTemplate, Map.of())));
    }
  }
}
