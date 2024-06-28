package org.int4.db.core.fluent;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class ExecutionStep<T, X extends Exception> implements MappingSteps<T, X>, TerminalMappingSteps<T, X> {
  private final Context<X> context;
  private final Function<SQLResult, Iterator<Row>> step;
  private final Function<Row, T> flatStep;

  ExecutionStep(Context<X> context, Function<SQLResult, Iterator<Row>> step, Function<Row, T> flatStep) {
    this.context = context;
    this.step = step;
    this.flatStep = flatStep;
  }

  @Override
  public <U> ExecutionStep<U, X> map(Function<T, U> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new ExecutionStep<>(context, step, r -> mapper.apply(flatStep.apply(r)));
  }

  @Override
  public boolean consume(Consumer<T> consumer, long max) throws X {
    return context.consume(r -> consumer.accept(flatStep.apply(r)), max, step::apply);
  }
}
