package org.int4.db.core.fluent;

import java.sql.PreparedStatement;

public interface Context<X extends Exception> {
  PreparedStatement createPreparedStatement() throws X;
  X wrapException(String message, Throwable cause);
}