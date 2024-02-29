package org.int4.db.core.fluent;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Context<X extends Exception> {
  PreparedStatement createPreparedStatement() throws X;
  X wrapException(String message, SQLException cause);
}