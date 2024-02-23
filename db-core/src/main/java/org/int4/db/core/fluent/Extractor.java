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
 */
public class Extractor<T> {  // TODO perhaps implement marker interface TemplateParameter ?
  private final List<String> names;

  final BiFunction<T, Integer, Object> dataExtractor;

  Extractor(List<String> names, BiFunction<T, Integer, Object> dataExtractor) {
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

  /**
   * Returns a list of field names, in which gaps are represented as
   * empty strings.
   *
   * @return a list of field names, never {@code null}
   */
  public List<String> names() {
    return names;
  }

  /**
   * Given a type {@code T}, provides a template parameter that inserts its fields
   * comma separated in the form of "field = value" suitable for UPDATE statements.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @return an entries template parameter, never {@code null}
   */
  public Entries entries(T t) {
    return new Entries(names, index -> dataExtractor.apply(t, index));
  }

  /**
   * Given a type {@code T}, provides a template parameter that inserts its values
   * comma separated, suitable for the INSERT statement's VALUES clause.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @return a values template parameter, never {@code null}
   */
  public Values values(T t) {
    return new Values(names, index -> dataExtractor.apply(t, index));
  }

  /**
   * Creates a new extractor based on this one, but excluding the given
   * names.
   *
   * @param names an array of names to exclude, cannot be {@code null}
   * @return a new extractor minus the given names, never {@code null}
   * @throws IllegalArgumentException when one or more names are missing
   */
  public Extractor<T> excluding(String... names) {
    Set<String> set = new LinkedHashSet<>(List.of(names));
    Extractor<T> extractor = new Extractor<>(this.names.stream().map(n -> set.remove(n) ? "" : n).toList(), dataExtractor);

    if(!set.isEmpty()) {
      throw new IllegalArgumentException("unable to exclude non-existing fields: " + set);
    }

    return extractor;
  }

  /**
   * Creates a new extractor based on this one, but keeping only the
   * given names.
   *
   * @param names an array of names to keep, cannot be {@code null}
   * @return a new extractor with only the given names, never {@code null}
   * @throws IllegalArgumentException when one or more names are missing
   */
  public Extractor<T> only(String... names) {
    Set<String> set = new LinkedHashSet<>(List.of(names));
    Extractor<T> extractor = new Extractor<>(this.names.stream().map(n -> set.remove(n) ? n : "").toList(), dataExtractor);

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
