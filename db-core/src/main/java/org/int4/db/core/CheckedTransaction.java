package org.int4.db.core;

import java.lang.StringTemplate.Processor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.StatementExecutor;

/**
 * Represents a database transaction.
 */
public class CheckedTransaction extends BaseTransaction<SQLException> implements Processor<StatementExecutor<SQLException>, SQLException> {

  CheckedTransaction(Supplier<Connection> connectionSupplier, boolean readOnly) {
    super(connectionSupplier, readOnly, (tx, msg, cause) -> new SQLException(tx + ": " + msg, cause));
  }

  @Override
  public StatementExecutor<SQLException> process(StringTemplate stringTemplate) {
    SafeSQL sql = new SafeSQL(stringTemplate);

    return new StatementExecutor<>(new Context<>(
      () -> {
        try {
          return sql.toPreparedStatement(getConnection());
        }
        catch(SQLException e) {
          throw new SQLExceptionWrapper(CheckedTransaction.this + ": creating statement failed for: " + sql, e);
        }
      },
      (message, cause) -> new SQLExceptionWrapper(CheckedTransaction.this + ": " + message, cause)
    ));
  }

  class SQLExceptionWrapper extends SQLException {
    SQLExceptionWrapper(String message, SQLException cause) {
      super(message, cause);
    }

    SQLException getSQLException() {
      return (SQLException)getCause();
    }
  }
}