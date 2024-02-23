package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;

import org.int4.db.core.util.JdbcFunction;

/**
 * Fluent executor for an SQL statement.
 *
 * @param <X> the exception type that can be thrown
 */
public class StatementExecutor<X extends Exception> implements RowSteps<X>, TerminalMappingSteps<Row, X> {
  private final Context<X> context;

  /**
   * Constructs a new instance.
   *
   * @param context a context, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public StatementExecutor(Context<X> context) {
    this.context = Objects.requireNonNull(context, "context");
  }

  /**
   * Switches the statement to the rows with generated keys returned by SQL {@code INSERT}
   * statements. Behavior is undefined if the statement is not an {@code INSERT} statement
   * or the statement returned no generated keys.
   *
   * @return a new intermediate step, never {@code null}
   */
  public RowSteps<X> mapGeneratedKeys() {
    return new RowsNode<>(context, PreparedStatement::getGeneratedKeys);
  }

  @Override
  public <R> ExecutionStep<R, X> map(JdbcFunction<Row, R> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new ExecutionStep<>(context, PreparedStatement::getResultSet, mapper);
  }

  /**
   * Executes the statement as a statement that returns a number of affected rows.
   * Calling this on a statement that returns rows, or nothing will result in an
   * exception.
   *
   * @return the number of affected rows, never negative
   * @throws X when execution fails
   */
  public long executeUpdate() throws X {
    try(PreparedStatement ps = context.createPreparedStatement()) {
      try {
        return ps.executeLargeUpdate();
      }
      catch(SQLException e) {
        throw context.wrapException("execution failed for: " + ps.toString(), e);
      }
    }
    catch(SQLException e) {
      throw context.wrapException("closing statement failed", e);
    }
  }

  /**
   * Executes the statement without expecting a result.
   *
   * @throws X when execution fails
   */
  public void execute() throws X {
    try(PreparedStatement ps = context.createPreparedStatement()) {
      try {
        ps.execute();
      }
      catch(SQLException e) {
        throw context.wrapException("execution failed for: " + ps.toString(), e);
      }
    }
    catch(SQLException e) {
      throw context.wrapException("closing statement failed", e);
    }
  }

  @Override
  public boolean consume(Consumer<Row> consumer, long max) throws X {
    if(max <= 0) {
      throw new IllegalArgumentException("max must be positive: " + max);
    }

    long rowsLeft = max;

    try(PreparedStatement ps = context.createPreparedStatement()) {
      try {
        ps.execute();

        try(ResultSet rs = ps.getResultSet()) {
          int columnCount = rs.getMetaData().getColumnCount();

          while(rs.next() && rowsLeft-- > 0) {
            Object[] row = new Object[columnCount];

            for(int i = 0; i < columnCount; i++) {
              row[i] = rs.getObject(i + 1);
            }

            consumer.accept(new StaticRow(row));
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