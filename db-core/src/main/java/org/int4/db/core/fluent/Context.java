package org.int4.db.core.fluent;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

import org.int4.db.core.reflect.Row;

public interface Context<X extends Exception> {
  void execute() throws X;
  long executeUpdate() throws X;
  boolean consume(Consumer<Row> consumer, long max, Function<SQLResult, Iterator<Row>> resultExtractor) throws X;
}