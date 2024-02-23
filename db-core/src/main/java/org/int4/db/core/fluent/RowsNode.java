package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;

import org.int4.db.core.util.JdbcFunction;

class RowsNode<X extends Exception> implements RowSteps<X> {
  private final Context<X> context;
  private final JdbcFunction<PreparedStatement, ResultSet> step;

  RowsNode(Context<X> context, JdbcFunction<PreparedStatement, ResultSet> step) {
    this.context = Objects.requireNonNull(context, "context");
    this.step = Objects.requireNonNull(step, "step");
  }

  @Override
  public <T> ExecutionStep<T, X> map(JdbcFunction<Row, T> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new ExecutionStep<>(context, step, mapper);
  }
}