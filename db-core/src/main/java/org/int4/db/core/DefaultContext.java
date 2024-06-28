package org.int4.db.core;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.Row;
import org.int4.db.core.fluent.RowAccessException;
import org.int4.db.core.fluent.SQLResult;
import org.int4.db.core.util.ThrowingSupplier;

class DefaultContext<X extends Exception> implements Context<X> {
  private final ThrowingSupplier<SQLStatement, X> preparedStatementSupplier;
  private final BiFunction<String, SQLException, X> exceptionWrapper;

  DefaultContext(ThrowingSupplier<SQLStatement, X> preparedStatementSupplier, BiFunction<String, SQLException, X> exceptionWrapper) {
    this.preparedStatementSupplier = preparedStatementSupplier;
    this.exceptionWrapper = exceptionWrapper;
  }

  @Override
  public void execute() throws X {
    execute(r -> null);
  }

  @Override
  public long executeUpdate() throws X {
    return execute(SQLResult::getUpdateCount);
  }

  @Override
  public boolean consume(Consumer<Row> consumer, long max, Function<SQLResult, Iterator<Row>> resultExtractor) throws X {
    Objects.requireNonNull(consumer, "consumer");

    if(max <= 0) {
      throw new IllegalArgumentException("max must be positive: " + max);
    }

    return execute(sr -> {
      Iterator<Row> iterator = resultExtractor.apply(sr);
      long rowsLeft = max;

      while(iterator.hasNext() && rowsLeft-- > 0) {
        consumer.accept(iterator.next());
      }

      return rowsLeft > 0 ? false : iterator.hasNext();
    });
  }

  private <R> R execute(Function<SQLResult, R> function) throws X {
    try(SQLStatement statement = preparedStatementSupplier.get()) {
      try {
        return function.apply(statement.execute());
      }
      catch(RowAccessException e) {
        throw exceptionWrapper.apply("execution failed for: " + statement.toString(), e.unwrap());
      }
      catch(SQLException e) {
        throw exceptionWrapper.apply("execution failed for: " + statement.toString(), e);
      }
    }
    catch(SQLException e) {
      throw exceptionWrapper.apply("closing statement failed", e);
    }
  }
}
