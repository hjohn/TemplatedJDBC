package org.int4.db.core.fluent;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.int4.db.core.internal.bridge.Context;
import org.int4.db.core.internal.bridge.SQLResult;
import org.int4.db.core.reflect.Row;

/**
 * Fluent node for a source that provide rows.
 *
 * @param <X> the exception type that can be thrown
 */
public class RowSourceNode<X extends Exception> implements RowSteps<X> {
  private final Context<X> context;
  private final Function<SQLResult, Iterator<Row>> step;

  RowSourceNode(Context<X> context, Function<SQLResult, Iterator<Row>> step) {
    this.context = Objects.requireNonNull(context, "context");
    this.step = Objects.requireNonNull(step, "step");
  }

  @Override
  public <T> MappedSourceNode<T, X> map(Function<Row, T> mapper) {
    Objects.requireNonNull(mapper, "mapper");

    return new MappedSourceNode<>(context, step, mapper);
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

        final Iterator<Row> delegate = step.apply(sr);

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