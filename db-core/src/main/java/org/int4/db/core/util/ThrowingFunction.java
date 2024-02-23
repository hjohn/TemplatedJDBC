package org.int4.db.core.util;

import java.util.Objects;

public interface ThrowingFunction<T, R, X extends Throwable> {
  R apply(T t) throws X;

  default <V> ThrowingFunction<T, V, X> andThen(ThrowingFunction<? super R, ? extends V, X> after) {
    Objects.requireNonNull(after);

    return t -> after.apply(apply(t));
  }
}
