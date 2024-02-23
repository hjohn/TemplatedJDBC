package org.int4.db.core.fluent;

class Reference<T> {
  private T value;

  void set(T value) {
    this.value = value;
  }

  T get() {
    return this.value;
  }
}
