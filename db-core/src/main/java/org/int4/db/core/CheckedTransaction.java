package org.int4.db.core;

import java.lang.StringTemplate.Processor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.StatementExecutor;

/**
 * Represents a database transaction.
 */
public class CheckedTransaction extends BaseTransaction<SQLException> implements Processor<StatementExecutor<SQLException>, SQLException> {
  private final BiFunction<CheckedTransaction, SafeSQL, Context<SQLException>> contextFactory;

  CheckedTransaction(Supplier<Connection> connectionSupplier, boolean readOnly, BiFunction<CheckedTransaction, SafeSQL, Context<SQLException>> contextFactory) {
    super(connectionSupplier, readOnly, (tx, msg, cause) -> new SQLException(tx + ": " + msg, cause));

    this.contextFactory = contextFactory;
  }

  @Override
  public StatementExecutor<SQLException> process(StringTemplate stringTemplate) {
    SafeSQL sql = new SafeSQL(stringTemplate);

    return new StatementExecutor<>(contextFactory.apply(this, sql));
  }
}