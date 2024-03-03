package org.int4.db.core.fluent;

import java.sql.SQLException;
import java.util.Objects;

/**
 * Thrown when accessing a {@link Row} fails.
 */
public class RowAccessException extends RuntimeException {
  private final SQLException e;

  /**
   * Constructs a new instance.
   *
   * @param e an SQLException, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public RowAccessException(SQLException e) {
    super(Objects.requireNonNull(e, "e"));

    this.e = e;
  }

  /**
   * Returns the {@link SQLException} this exception wrapped.
   *
   * @return the {@link SQLException} this exception wrapped, never {@code null}
   */
  public SQLException unwrap() {
    return e;
  }
}