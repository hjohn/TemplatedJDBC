package org.int4.db.core.fluent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.int4.db.core.util.JdbcConsumer;

/**
 * Provides terminal operations that will trigger statement execution.
 *
 * @param <T> the type of the result
 * @param <X> the type of exception that can be thrown
 */
public interface TerminalMappingSteps<T, X extends Exception> {

  /**
   * Returns the first result, or {@code null} if there were no results.
   *
   * @return the first result, or {@code null} if there were no results
   * @throws X when an exception occurs
   */
  default T getFirst() throws X {
    Reference<T> reference = new Reference<>();

    consume(reference::set, 1);

    return reference.get();
  }

  /**
   * Checks if there were exactly 0 or 1 results, and returns the only result or
   * {@code null} if there were none. This method will throw an exception if
   * there was more than one result available.
   *
   * @return the only result, or {@code null} if there were no results
   * @throws IllegalStateException when there was more than a single result available
   * @throws X when an exception occurs
   */
  default T get() throws X {
    Reference<T> reference = new Reference<>();

    boolean moreAvailable = consume(reference::set, 1);

    if(moreAvailable) {
      throw new IllegalStateException("expected a single result, but more than one result was available");
    }

    return reference.get();
  }

  /**
   * Checks if there were exactly 0 or 1 results, and returns the result as an
   * optional. This method will throw an exception if there was more than one
   * result available.
   *
   * @return an optional result, never {@code null}
   * @throws IllegalStateException when there was more than a single result available
   * @throws X when an exception occurs
   */
  default Optional<T> getOptional() throws X {
    return Optional.ofNullable(get());
  }

  /**
   * Returns the results as a list.
   *
   * @return a list of results, never {@code null} or contains {@code null}, but can be empty
   * @throws X when an exception occurs
   */
  default List<T> toList() throws X {
    List<T> list = new ArrayList<>();

    consume(list::add);

    return list;
  }

  /**
   * Consumes the results using the given consumer.
   *
   * @param consumer a consumer for processing each result, cannot be {@code null}
   * @throws X when an exception occurs
   * @throws NullPointerException when any argument is {@code null}
   */
  default void consume(JdbcConsumer<T> consumer) throws X {
    consume(consumer, Long.MAX_VALUE);
  }

  /**
   * Consumes a given maximum number of results using the given consumer, and
   * returns whether there were more results left to consume if the maximum was
   * reached.
   *
   * @param consumer a consumer for processing each result, cannot be {@code null}
   * @param max a maximum number of results to consume, must be positive
   * @return {@code true} if there were more results left to consume, otherwise {@code false}
   * @throws X when an exception occurs
   * @throws NullPointerException when any argument is {@code null}
   */
  boolean consume(JdbcConsumer<T> consumer, long max) throws X;
}
