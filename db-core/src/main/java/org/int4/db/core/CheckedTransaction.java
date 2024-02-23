package org.int4.db.core;

import java.lang.StringTemplate.Processor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.StatementExecutor;

/**
 * Represents a database transaction.
 */
public class CheckedTransaction extends BaseTransaction<SQLException> implements Processor<StatementExecutor<SQLException>, SQLException> {

  CheckedTransaction(Supplier<Connection> connectionProvider, boolean readOnly) throws SQLException {
    super(connectionProvider, readOnly, (tx, msg, cause) -> new SQLException(tx + ": " + msg, cause));
  }

  @Override
  public StatementExecutor<SQLException> process(StringTemplate stringTemplate) {
    SafeSQL sql = new SafeSQL(stringTemplate);

    return new StatementExecutor<>(new Context<>() {
      @Override
      public SQLException wrapException(String message, Throwable cause) {
        return new SQLException(CheckedTransaction.this + ": " + message, cause);
      }

      @Override
      public PreparedStatement createPreparedStatement() throws SQLException {
        try {
          ensureNotFinished();

          return sql.toPreparedStatement(connection);
        }
        catch(SQLException e) {
          throw new SQLException(CheckedTransaction.this + ": creating statement failed for: " + sql, e);
        }
      }
    });
  }
}