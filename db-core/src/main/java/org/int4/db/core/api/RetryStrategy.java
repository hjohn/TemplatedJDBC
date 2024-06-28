package org.int4.db.core.api;

import java.sql.SQLException;

/**
 * Strategy that determines when to retry a database operation.
 *
 * <p>The strategy is provided with an {@link SQLException} which can be
 * examined to determine the root cause of the error, and whether or not it
 * warrants a retry.
 */
public interface RetryStrategy {

  /**
   * A strategy that never retries operations.
   */
  static final RetryStrategy NONE = (failCount, throwable) -> false;

  /**
   * Determines whether an operation should be retried given the number
   * of failures so far and a failure cause.
   *
   * <p>Note: if retries should not be performed immediately, then the strategy
   * should block an appropriate period before returning from this call.
   *
   * @param failCount the number of failures so far, always positive
   * @param exception the failure cause, never {@code null}
   * @return {@code true} if the operation should be retried, otherwise {@code false}
   */
  boolean retry(int failCount, SQLException exception);
}
