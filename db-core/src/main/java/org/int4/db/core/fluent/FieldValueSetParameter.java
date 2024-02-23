package org.int4.db.core.fluent;

import java.util.List;
import java.util.function.Function;

public sealed abstract class FieldValueSetParameter {
  private final List<String> names;
  private final Function<Integer, Object> dataSource;

  FieldValueSetParameter(List<String> names, Function<Integer, Object> dataSource) {
    this.names = List.copyOf(names);
    this.dataSource = dataSource;
  }

  public List<String> names() {
    return names;
  }

  public int size() {
    return names.size();
  }

  public String getName(int index) {
    return names.get(index);
  }

  public Object getValue(int index) {
    return dataSource.apply(index);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[names=" + names + ", dataSource=" + dataSource + "]";
  }

  public static final class Values extends FieldValueSetParameter {
    Values(List<String> names, Function<Integer, Object> dataSource) {
      super(names, dataSource);
    }
  }

  public static final class Entries extends FieldValueSetParameter {
    Entries(List<String> names, Function<Integer, Object> dataSource) {
      super(names, dataSource);
    }
  }
}
