package org.int4.db.core.util;

public interface ThrowingConsumer<T, X extends Throwable> {
  void accept(T t) throws X;
}
