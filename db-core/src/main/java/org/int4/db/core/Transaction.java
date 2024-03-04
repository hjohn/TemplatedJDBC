package org.int4.db.core;

import java.lang.StringTemplate.Processor;
import java.sql.Connection;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.StatementExecutor;

/**
 * Represents a database transaction.
 */
public class Transaction extends BaseTransaction<DatabaseException> implements Processor<StatementExecutor<DatabaseException>, DatabaseException> {
  private final BiFunction<Transaction, SafeSQL, Context<DatabaseException>> contextFactory;

  public Transaction(Supplier<Connection> connectionProvider, boolean readOnly, BiFunction<Transaction, SafeSQL, Context<DatabaseException>> contextFactory) {
    super(connectionProvider, readOnly, (tx, msg, cause) -> new DatabaseException(tx + ": " + msg, cause));

    this.contextFactory = contextFactory;
  }

  @Override
  public StatementExecutor<DatabaseException> process(StringTemplate stringTemplate) {
    SafeSQL sql = new SafeSQL(stringTemplate);

    return new StatementExecutor<>(contextFactory.apply(this, sql));
  }
}