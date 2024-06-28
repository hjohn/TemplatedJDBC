package org.int4.db.core.fluent;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

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
    return new RowsNode<>(context, SQLResult::createGeneratedKeysIterator);
  }

  @Override
  public <R> ExecutionStep<R, X> map(Function<Row, R> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new ExecutionStep<>(context, SQLResult::createIterator, mapper);
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
    return context.executeUpdate();
  }

  /**
   * Executes the statement without expecting a result.
   *
   * @throws X when execution fails
   */
  public void execute() throws X {
    context.execute();
  }

  @Override
  public boolean consume(Consumer<Row> consumer, long max) throws X {
    return context.consume(
      consumer,
      max,
      sr -> new Iterator<>() {

        /*
         * This wraps the internal Iterator because it re-uses rows. As
         * in this case Rows are returned all at once, they must retain their
         * values even after iteration completes.
         */

        final Iterator<Row> delegate = sr.createIterator();

        @Override
        public boolean hasNext() {
          return delegate.hasNext();
        }

        @Override
        public Row next() {
          Row row = delegate.next();
          Object[] data = new Object[row.getColumnCount()];

          for(int i = 0; i < data.length; i++) {
            data[i] = row.getObject(i);
          }

          return Row.of(data);
        }
      }
    );
  }
}