package org.int4.db.test;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.int4.db.core.api.DatabaseException;
import org.int4.db.core.api.RetryStrategy;
import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.SQLResult;
import org.int4.db.core.internal.SafeSQL;
import org.int4.db.core.reflect.Row;
import org.int4.db.core.reflect.RowAccessException;
import org.int4.db.core.util.ThrowingRunnable;
import org.int4.db.core.util.ThrowingSupplier;

abstract class AbstractMockDatabase<X extends Exception> {
  private static final Logger LOGGER = System.getLogger(AbstractMockDatabase.class.getName());

  private final Map<Pattern, ThrowingSupplier<List<Row>, X>> queryMocks = new HashMap<>();
  private final Map<Pattern, ThrowingSupplier<Long, X>> updateMocks = new HashMap<>();
  private final Map<Pattern, ThrowingRunnable<X>> executeMocks = new HashMap<>();

  /**
   * Mocks a query, returning the given rows when a query was executed that
   * matches the given regular expression.
   *
   * @param regex a regular expression, cannot be {@code null}
   * @param rows a list of rows to return, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public void mockQuery(String regex, List<Row> rows) {
    Objects.requireNonNull(rows, "rows");

    mockQuery(regex, () -> rows);
  }

  /**
   * Mocks a query, calling the given supplier to supply rows when a query was
   * executed that matches the given regular expression. If the supplier throws
   * an exception, it is treated as if the database threw the exception.
   *
   * @param regex a regular expression, cannot be {@code null}
   * @param supplier a row supplier, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public void mockQuery(String regex, ThrowingSupplier<List<Row>, X> supplier) {
    queryMocks.put(Pattern.compile(regex), Objects.requireNonNull(supplier, "supplier"));
  }

  /**
   * Mocks an update type statement, returning the given affected row count when
   * a statement was executed that matches the given regular expression.
   *
   * @param regex a regular expression, cannot be {@code null}
   * @param count a count to return
   * @throws NullPointerException when any argument is {@code null}
   */
  public void mockUpdate(String regex, long count) {
    mockUpdate(regex, () -> count);
  }

  /**
   * Mocks an update type statement, calling the given supplier to supply an
   * affected row count when a statement was executed that matches the given
   * regular expression. If the supplier throws an exception, it is treated as
   * if the database threw the exception.
   *
   * @param regex a regular expression, cannot be {@code null}
   * @param supplier an affect row count supplier, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public void mockUpdate(String regex, ThrowingSupplier<Long, X> supplier) {
    updateMocks.put(Pattern.compile(regex), Objects.requireNonNull(supplier, "supplier"));
  }

  /**
   * Mocks a statement that returns no results, calling the given runnable when
   * a statement was executed that matches the given regular expression. If the
   * runnable throws an exception, it is treated as if the database threw the
   * exception.
   *
   * @param regex a regular expression, cannot be {@code null}
   * @param runnable a runnable, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public void mockExecute(String regex, ThrowingRunnable<X> runnable) {
    executeMocks.put(Pattern.compile(regex), Objects.requireNonNull(runnable, "runnable"));
  }

  public RetryStrategy retryStrategy() {
    return RetryStrategy.NONE;
  }

  MockContext createContext(SafeSQL sql) {
    return new MockContext(sql);
  }

  class MockContext implements Context<X> {
    private final SafeSQL sql;

    MockContext(SafeSQL sql) {
      this.sql = sql;
    }

    @Override
    public void execute() throws X {
      String statement = sql.getSQL();

      for(Pattern pattern : executeMocks.keySet()) {
        if(pattern.matcher(statement).matches()) {
          LOGGER.log(Level.INFO, "Mocking statement: '" + pattern + "' matched '" + statement + "'");

          executeMocks.get(pattern).run();
          break;
        }
      }

      LOGGER.log(Level.WARNING, "No mock defined for: '" + statement + "'");
    }

    @Override
    public long executeUpdate() throws X {
      String statement = sql.getSQL();

      for(Pattern pattern : updateMocks.keySet()) {
        if(pattern.matcher(statement).matches()) {
          LOGGER.log(Level.INFO, "Mocking update: '" + pattern + "' matched '" + statement + "'");

          return updateMocks.get(pattern).get();
        }
      }

      LOGGER.log(Level.WARNING, "No mock defined for: '" + statement + "'");

      return 0;
    }

    @Override
    public boolean consume(Consumer<Row> consumer, long max, Function<SQLResult, Iterator<Row>> resultExtractor) throws X {
      String statement = sql.getSQL();

      for(Pattern pattern : queryMocks.keySet()) {
        if(pattern.matcher(statement).matches()) {
          LOGGER.log(Level.INFO, "Mocking query: '" + pattern + "' matched '" + statement + "'");

          long rowsLeft = max;

          Iterator<Row> iterator = queryMocks.get(pattern).get().iterator();

          while(iterator.hasNext() && rowsLeft-- > 0) {
            try {
              consumer.accept(iterator.next());
            }
            catch(RowAccessException e) {
              throw new DatabaseException("execution failed for: " + statement, e.unwrap());
            }
          }

          return rowsLeft > 0 ? false : iterator.hasNext();
        }
      }

      LOGGER.log(Level.WARNING, "No mock defined for: '" + statement + "'");

      return false;
    }
  }
}
