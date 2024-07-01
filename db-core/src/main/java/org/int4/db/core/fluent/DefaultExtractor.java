package org.int4.db.core.fluent;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.int4.db.core.fluent.FieldValueSetParameter.Entries;
import org.int4.db.core.fluent.FieldValueSetParameter.Values;

/**
 * Provides template parameters providing field and field/value
 * combinations.
 *
 * @param <T> the type being extracted from
 */
class DefaultExtractor<T> implements Extractor<T> {
  private final List<String> names;
  private final BiFunction<T, Integer, Object> dataExtractor;

  DefaultExtractor(List<String> names, BiFunction<T, Integer, Object> dataExtractor) {
    this.names = List.copyOf(names);
    this.dataExtractor = dataExtractor;

    Set<String> uniqueNames = new HashSet<>();

    for(String name : names) {
      if(!name.isEmpty()) {
        if(!Identifier.isValidIdentifier(name)) {
          throw new IllegalArgumentException("names must only contain valid identifiers: " + name);
        }
        if(!uniqueNames.add(name)) {
          throw new IllegalArgumentException("names cannot contain duplicate names, but found duplicate: " + name + " in: " + names);
        }
      }
    }
  }

  @Override
  public Object extractObject(T t, int columnIndex) {
    return dataExtractor.apply(t, columnIndex);
  }

  @Override
  public List<String> names() {
    return names;
  }

  @Override
  public Entries entries(T t) {
    return new Entries(names, index -> dataExtractor.apply(t, index));
  }

  @Override
  public Values values(T t) {
    return new Values(names, index -> dataExtractor.apply(t, index));
  }

  @Override
  public Extractor<T> excluding(String... names) {
    Set<String> set = new LinkedHashSet<>(List.of(names));
    Extractor<T> extractor = new DefaultExtractor<>(this.names.stream().map(n -> set.remove(n) ? "" : n).toList(), dataExtractor);

    if(!set.isEmpty()) {
      throw new IllegalArgumentException("unable to exclude non-existing fields: " + set + ", available are: " + this.names);
    }

    return extractor;
  }

  @Override
  public Extractor<T> only(String... names) {
    Set<String> set = new LinkedHashSet<>(List.of(names));
    Extractor<T> extractor = new DefaultExtractor<>(this.names.stream().map(n -> set.remove(n) ? n : "").toList(), dataExtractor);

    if(!set.isEmpty()) {
      throw new IllegalArgumentException("unable to keep non-existing fields: " + set);
    }

    return extractor;
  }

  @Override
  public String toString() {
    return names.toString();
  }
}
