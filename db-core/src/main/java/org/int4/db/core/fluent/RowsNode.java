package org.int4.db.core.fluent;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

class RowsNode<X extends Exception> implements RowSteps<X> {
  private final Context<X> context;
  private final Function<SQLResult, Iterator<Row>> step;

  RowsNode(Context<X> context, Function<SQLResult, Iterator<Row>> step) {
    this.context = Objects.requireNonNull(context, "context");
    this.step = Objects.requireNonNull(step, "step");
  }

  @Override
  public <T> ExecutionStep<T, X> map(Function<Row, T> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new ExecutionStep<>(context, step, mapper);
  }
}