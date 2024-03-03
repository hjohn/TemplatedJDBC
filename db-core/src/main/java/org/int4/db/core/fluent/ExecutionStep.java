package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.int4.db.core.util.JdbcFunction;
import org.int4.db.core.util.JdbcIterator;

public class ExecutionStep<T, X extends Exception> implements MappingSteps<T, X>, TerminalMappingSteps<T, X> {
  private final Context<X> context;
  private final JdbcFunction<PreparedStatement, ResultSet> step;
  private final Function<Row, T> flatStep;

  ExecutionStep(Context<X> context, JdbcFunction<PreparedStatement, ResultSet> step, Function<Row, T> flatStep) {
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
    return context.consume(
      r -> consumer.accept(flatStep.apply(r)),
      max,
      ps -> new JdbcIterator<>() {
        final ResultSet rs = step.apply(ps);
        final DynamicRow row = new DynamicRow(rs);

        @Override
        public boolean next() throws SQLException {
          return rs.next();
        }

        @Override
        public Row get() {
          return row;
        }
      }
    );
  }
}
