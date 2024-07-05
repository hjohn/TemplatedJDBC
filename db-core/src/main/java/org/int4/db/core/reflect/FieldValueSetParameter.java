package org.int4.db.core.reflect;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public sealed abstract class FieldValueSetParameter {
  private final List<String> names;
  private final int batchSze;
  private final BiFunction<Integer, Integer, Object> dataSource;

  FieldValueSetParameter(List<String> names, int batchSize, BiFunction<Integer, Integer, Object> dataSource) {
    this.names = List.copyOf(names);
    this.batchSze = batchSize;
    this.dataSource = dataSource;
  }

  public List<String> names() {
    return names;
  }

  public int size() {
    return names.size();
  }

  public int batchSize() {
    return batchSze;
  }

  public String getName(int index) {
    return names.get(index);
  }

  public Object getValue(int row, int index) {
    return dataSource.apply(row, index);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[names=" + names + ", dataSource=" + dataSource + "]";
  }

  public static final class Values extends FieldValueSetParameter {
    Values(List<String> names, Function<Integer, Object> dataSource) {
      super(names, 1, (row, index) -> dataSource.apply(index));
    }

    Values(List<String> names, int size, BiFunction<Integer, Integer, Object> dataSource) {
      super(names, size, dataSource);
    }
  }

  public static final class Entries extends FieldValueSetParameter {
    Entries(List<String> names, Function<Integer, Object> dataSource) {
      super(names, 1, (row, index) -> dataSource.apply(index));
    }
  }
}
