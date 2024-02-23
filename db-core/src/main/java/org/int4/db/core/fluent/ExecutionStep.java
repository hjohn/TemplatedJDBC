package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;

import org.int4.db.core.util.JdbcFunction;

public class ExecutionStep<T, X extends Exception> implements MappingSteps<T, X>, TerminalMappingSteps<T, X> {
  private final Context<X> context;
  private final JdbcFunction<PreparedStatement, ResultSet> step;
  private final JdbcFunction<Row, T> flatStep;

  ExecutionStep(Context<X> context, JdbcFunction<PreparedStatement, ResultSet> step, JdbcFunction<Row, T> flatStep) {
    this.context = context;
    this.step = step;
    this.flatStep = flatStep;
  }

  @Override
  public <U> ExecutionStep<U, X> map(JdbcFunction<T, U> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new ExecutionStep<>(context, step, r -> mapper.apply(flatStep.apply(r)));
  }

  @Override
  public boolean consume(Consumer<T> consumer, long max) throws X {
    Objects.requireNonNull(consumer, "consumer");

    if(max <= 0) {
      throw new IllegalArgumentException("max must be positive: " + max);
    }

    long rowsLeft = max;

    try(PreparedStatement ps = context.createPreparedStatement()) {
      try {
        ps.execute();

        try(ResultSet rs = step.apply(ps)) {
          DynamicRow row = new DynamicRow(rs);

          while(rs.next() && rowsLeft-- > 0) {
            consumer.accept(flatStep.apply(row));
          }

          return rowsLeft > 0 ? false : rs.next();
        }
      }
      catch(SQLException e) {
        throw context.wrapException("execution failed for: " + ps.toString(), e);
      }
    }
    catch(SQLException e) {
      throw context.wrapException("closing statement failed", e);
    }
  }
}
