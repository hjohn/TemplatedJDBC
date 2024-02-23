package org.int4.db.core.fluent;

import org.int4.db.core.util.JdbcFunction;

interface RowSteps<X extends Exception> extends MappingSteps<Row, X> {
  static final JdbcFunction<Row, String> TEXT_MAPPER = r -> r.getString(0);
  static final JdbcFunction<Row, Integer> INT_MAPPER = r -> r.getInt(0);
  static final JdbcFunction<Row, Long> LONG_MAPPER = r -> r.getLong(0);
  static final JdbcFunction<Row, byte[]> BYTES_MAPPER = r -> r.getBytes(0);

  default ExecutionStep<String, X> asString() {
    return map(TEXT_MAPPER);
  }

  default ExecutionStep<Integer, X> asInt() {
    return map(INT_MAPPER);
  }

  default ExecutionStep<Long, X> asLong() {
    return map(LONG_MAPPER);
  }

  default ExecutionStep<byte[], X> asBytes() {
    return map(BYTES_MAPPER);
  }
}