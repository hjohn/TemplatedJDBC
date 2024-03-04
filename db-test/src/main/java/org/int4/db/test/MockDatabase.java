package org.int4.db.test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.int4.db.core.Database;
import org.int4.db.core.DatabaseException;
import org.int4.db.core.RetryStrategy;
import org.int4.db.core.SafeSQL;
import org.int4.db.core.Transaction;
import org.int4.db.core.fluent.Context;
import org.int4.db.core.fluent.Row;
import org.int4.db.core.fluent.RowAccessException;
import org.int4.db.core.util.JdbcFunction;
import org.int4.db.core.util.JdbcIterator;
import org.int4.db.core.util.ThrowingRunnable;
import org.int4.db.core.util.ThrowingSupplier;

/**
 * A database which can return mocked responses when SQL statements match a
 * regular expression.
 */
public class MockDatabase implements Database {
  private final Map<Pattern, ThrowingSupplier<List<Row>, DatabaseException>> queryMocks = new HashMap<>();
  private final Map<Pattern, ThrowingSupplier<Long, DatabaseException>> updateMocks = new HashMap<>();
  private final Map<Pattern, ThrowingRunnable<DatabaseException>> executeMocks = new HashMap<>();

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
  public void mockQuery(String regex, ThrowingSupplier<List<Row>, DatabaseException> supplier) {
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
  public void mockUpdate(String regex, ThrowingSupplier<Long, DatabaseException> supplier) {
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
  public void mockExecute(String regex, ThrowingRunnable<DatabaseException> runnable) {
    executeMocks.put(Pattern.compile(regex), Objects.requireNonNull(runnable, "runnable"));
  }

  @Override
  public Transaction beginTransaction(boolean readOnly) throws DatabaseException {
    return new Transaction(() -> null, readOnly, (tx, sql) -> new MockContext(sql));
  }

  @Override
  public RetryStrategy retryStrategy() {
    return RetryStrategy.NONE;
  }

  @Override
  public SQLException unwrap(DatabaseException exception) {
    return exception.getSQLException();
  }

  @Override
  public Class<DatabaseException> exceptionType() {
    return DatabaseException.class;
  }

  private class MockContext implements Context<DatabaseException> {
    private final SafeSQL sql;

    MockContext(SafeSQL sql) {
      this.sql = sql;
    }

    @Override
    public void execute() throws DatabaseException {
      String statement = sql.getSQL();

      for(Pattern pattern : executeMocks.keySet()) {
        if(pattern.matcher(statement).matches()) {
          executeMocks.get(pattern).run();
          break;
        }
      }
    }

    @Override
    public long executeUpdate() throws DatabaseException {
      String statement = sql.getSQL();

      for(Pattern pattern : updateMocks.keySet()) {
        if(pattern.matcher(statement).matches()) {
          return updateMocks.get(pattern).get();
        }
      }

      return 0;
    }

    @Override
    public boolean consume(Consumer<Row> consumer, long max, JdbcFunction<PreparedStatement, JdbcIterator<Row>> resultSetExtractor) throws DatabaseException {
      String statement = sql.getSQL();

      for(Pattern pattern : queryMocks.keySet()) {
        if(pattern.matcher(statement).matches()) {
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

      return false;
    }
  }
}
