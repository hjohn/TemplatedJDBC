package org.int4.db.core.fluent;

import org.int4.db.core.reflect.Mapper;
import org.int4.db.core.reflect.Row;

/**
 * Provides steps to convert {@link Row}s to a new type of result.
 *
 * @param <X> the type of exception that can be thrown
 */
public interface RowSteps<X extends Exception> extends MappingSteps<Row, X> {

  /**
   * A mapper which converts the result to a {@link String} obtained
   * from the first column.
   */
  static final Mapper<String> TEXT_MAPPER = r -> r.getString(0);

  /**
   * A mapper which converts the result to an {@link Integer} obtained
   * from the first column.
   */
  static final Mapper<Integer> INT_MAPPER = r -> r.getInt(0);

  /**
   * A mapper which converts the result to a {@link Long} obtained
   * from the first column.
   */
  static final Mapper<Long> LONG_MAPPER = r -> r.getLong(0);

  /**
   * A mapper which converts the result to a byte array obtained
   * from the first column.
   */
  static final Mapper<byte[]> BYTES_MAPPER = r -> r.getBytes(0);

  /**
   * Converts the first column of the result to a {@link String}.
   *
   * @return a new intermediate step, never {@code null}
   */
  default ExecutionStep<String, X> asString() {
    return map(TEXT_MAPPER);
  }

  /**
   * Converts the first column of the result to an {@link Integer}.
   *
   * @return a new intermediate step, never {@code null}
   */
  default ExecutionStep<Integer, X> asInt() {
    return map(INT_MAPPER);
  }

  /**
   * Converts the first column of the result to a {@link Long}.
   *
   * @return a new intermediate step, never {@code null}
   */
  default ExecutionStep<Long, X> asLong() {
    return map(LONG_MAPPER);
  }

  /**
   * Converts the first column of the result to a byte array.
   *
   * @return a new intermediate step, never {@code null}
   */
  default ExecutionStep<byte[], X> asBytes() {
    return map(BYTES_MAPPER);
  }
}