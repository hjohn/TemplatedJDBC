package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.int4.db.core.util.JdbcFunction;

public class RowsNode<X extends Exception> implements RowSteps<X> {
  private final Context<X> context;
  private final JdbcFunction<PreparedStatement, ResultSet> step;

  public RowsNode(Context<X> context, JdbcFunction<PreparedStatement, ResultSet> step) {
    this.context = context;
    this.step = step;
  }

  @Override
  public <T> ExecutionStep<T, X> map(JdbcFunction<Row, T> mapper) {
    return new ExecutionStep<>(context, step, mapper);
  }
}