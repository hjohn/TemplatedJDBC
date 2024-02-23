package org.int4.db.core;

import java.lang.StringTemplate.Processor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.StatementExecutor;

public class Transaction extends BaseTransaction<DatabaseException> implements Processor<StatementExecutor<DatabaseException>, DatabaseException> {

  Transaction(Supplier<Connection> connectionProvider, boolean readOnly) {
    super(connectionProvider, readOnly, (tx, msg, cause) -> new DatabaseException(tx + ": " + msg, cause));
  }

  @Override
  public StatementExecutor<DatabaseException> process(StringTemplate stringTemplate) {
    SafeSQL sql = new SafeSQL(stringTemplate);

    return new StatementExecutor<>(new Context<>() {
      @Override
      public DatabaseException wrapException(String message, Throwable cause) {
        return new DatabaseException(Transaction.this + ": " + message, cause);
      }

      @Override
      public PreparedStatement createPreparedStatement() {
        try {
          ensureNotFinished();

          return sql.toPreparedStatement(Transaction.this.connection);
        }
        catch(SQLException e) {
          throw new DatabaseException(Transaction.this + ": creating statement failed for: " + sql, e);
        }
      }
    });
  }
}