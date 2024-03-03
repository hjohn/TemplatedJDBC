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
public class Transaction extends BaseTransaction<DatabaseException> implements Processor<StatementExecutor<DatabaseException>, DatabaseException> {

  Transaction(Supplier<Connection> connectionProvider, boolean readOnly) {
    super(connectionProvider, readOnly, (tx, msg, cause) -> new DatabaseException(tx + ": " + msg, cause));
  }

  @Override
  public StatementExecutor<DatabaseException> process(StringTemplate stringTemplate) {
    SafeSQL sql = new SafeSQL(stringTemplate);

    return new StatementExecutor<>(new Context<>(
      () -> createPreparedStatement(sql),
      (message, cause) -> new DatabaseException(Transaction.this + ": " + message, cause)
    ));
  }

  @SuppressWarnings("resource")
  private PreparedStatement createPreparedStatement(SafeSQL sql) {
    try {
      return sql.toPreparedStatement(getConnection());
    }
    catch(SQLException e) {
      throw new DatabaseException(Transaction.this + ": creating statement failed for: " + sql, e);
    }
  }
}