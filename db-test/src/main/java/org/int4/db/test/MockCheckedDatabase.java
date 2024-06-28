package org.int4.db.test;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;

import org.int4.db.core.api.CheckedDatabase;
import org.int4.db.core.api.CheckedTransaction;
import org.int4.db.core.api.TransactionResult;
import org.int4.db.core.fluent.StatementExecutor;
import org.int4.db.core.internal.SafeSQL;

/**
 * A database which can return mocked responses when SQL statements match a
 * regular expression.
 */
public class MockCheckedDatabase extends AbstractMockDatabase<SQLException> implements CheckedDatabase {

  @Override
  public CheckedTransaction beginTransaction(boolean readOnly) {
    return new InternalTransaction();
  }

  @Override
  public SQLException unwrap(SQLException exception) {
    return exception;
  }

  @Override
  public Class<SQLException> exceptionType() {
    return SQLException.class;
  }

  class InternalTransaction implements CheckedTransaction {

    @Override
    public void commit() {
    }

    @Override
    public void rollback() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addCompletionHook(Consumer<TransactionResult> consumer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

    @Override
    public StatementExecutor<SQLException> process(StringTemplate stringTemplate) {
      return new StatementExecutor<>(createContext(new SafeSQL(stringTemplate, Map.of())));
    }
  }
}
