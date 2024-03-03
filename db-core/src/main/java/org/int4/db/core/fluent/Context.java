package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.int4.db.core.util.JdbcFunction;
import org.int4.db.core.util.JdbcIterator;
import org.int4.db.core.util.ThrowingSupplier;

public class Context<X extends Exception> {
  private final ThrowingSupplier<PreparedStatement, X> preparedStatementSupplier;
  private final BiFunction<String, SQLException, X> exceptionWrapper;

  public Context(ThrowingSupplier<PreparedStatement, X> preparedStatementSupplier, BiFunction<String, SQLException, X> exceptionWrapper) {
    this.preparedStatementSupplier = preparedStatementSupplier;
    this.exceptionWrapper = exceptionWrapper;
  }

  public void execute() throws X {
    execute(ps -> null);
  }

  public long executeUpdate() throws X {
    return execute(PreparedStatement::getLargeUpdateCount);
  }

  public boolean consume(Consumer<Row> consumer, long max, JdbcFunction<PreparedStatement, JdbcIterator<Row>> resultSetExtractor) throws X {
    Objects.requireNonNull(consumer, "consumer");

    if(max <= 0) {
      throw new IllegalArgumentException("max must be positive: " + max);
    }

    return execute(ps -> {
      try {
        long rowsLeft = max;

        JdbcIterator<Row> iterator = resultSetExtractor.apply(ps);

        while(iterator.next() && rowsLeft-- > 0) {
          consumer.accept(iterator.get());
        }

        return rowsLeft > 0 ? false : iterator.next();
      }
      catch(RowAccessException e) {
        throw e.unwrap();
      }
    });
  }

  private <R> R execute(JdbcFunction<PreparedStatement, R> function) throws X {
    try(PreparedStatement ps = preparedStatementSupplier.get()) {
      try {
        ps.execute();

        return function.apply(ps);
      }
      catch(SQLException e) {
        throw exceptionWrapper.apply("execution failed for: " + ps.toString(), e);
      }
    }
    catch(SQLException e) {
      throw exceptionWrapper.apply("closing statement failed", e);
    }
  }
}