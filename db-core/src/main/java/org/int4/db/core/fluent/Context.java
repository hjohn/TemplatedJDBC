package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

import org.int4.db.core.util.JdbcFunction;
import org.int4.db.core.util.JdbcIterator;

public interface Context<X extends Exception> {
  void execute() throws X;
  long executeUpdate() throws X;
  boolean consume(Consumer<Row> consumer, long max, JdbcFunction<PreparedStatement, JdbcIterator<Row>> resultSetExtractor) throws X;
}