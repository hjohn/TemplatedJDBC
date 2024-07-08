package org.int4.db.core.reflect;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.int4.db.core.reflect.FieldValueSetParameter.Entries;
import org.int4.db.core.reflect.FieldValueSetParameter.Values;
import org.int4.db.core.util.ColumnExtractor;

/**
 * Provides template parameters providing field and field/value
 * combinations.
 *
 * @param <T> the type being extracted from
 */
public sealed interface Extractor<T> permits Reflector, DefaultExtractor {

  /**
   * Returns a list of field names, in which gaps are represented as
   * empty strings.
   *
   * @return a list of field names, never {@code null}
   */
  List<String> names();

  /**
   * Returns the used {@link ColumnExtractor}.
   *
   * @return the used {@link ColumnExtractor}, never {@code null}
   */
  ColumnExtractor<T> columnExtractor();

  /**
   * Given a type {@code T}, provides a template parameter that inserts its fields
   * comma separated in the form of "field = value" suitable for UPDATE statements.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @return an entries template parameter, never {@code null}
   */
  default Entries entries(T t) {
    return new Entries(names(), index -> columnExtractor().extract(t, index));
  }

  /**
   * Given a type {@code T}, provides a template parameter that inserts its values
   * comma separated, suitable for the INSERT statement's VALUES clause.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @return a values template parameter, never {@code null}
   */
  default Values values(T t) {
    return new Values(names(), index -> columnExtractor().extract(t, index));
  }

  /**
   * Given a list of type {@code T}, provides a template parameter that inserts
   * its values comma separated, suitable for the INSERT statement's VALUES clause.
   * The statement will be executed as a batch.
   *
   * @param batch a list of type {@code T}, cannot be {@code null}
   * @return a values template parameter, never {@code null}
   * @throws NullPointerException when {@code batch} is {@code null}
   * @throws IllegalArgumentException when {@code batch} is empty
   */
  default Values batch(List<T> batch) {
    if(Objects.requireNonNull(batch, "batch").isEmpty()) {
      throw new IllegalArgumentException("batch cannot be empty");
    }

    return new Values(names(), batch.size(), (rowIndex, columnIndex) -> columnExtractor().extract(batch.get(rowIndex), columnIndex));
  }

  /**
   * Given an array of type {@code T}, provides a template parameter that inserts
   * its values comma separated, suitable for the INSERT statement's VALUES clause.
   * The statement will be executed as a batch.
   *
   * @param batch an array of type {@code T}, cannot be {@code null}
   * @return a values template parameter, never {@code null}
   * @throws NullPointerException when {@code batch} is {@code null}
   * @throws IllegalArgumentException when {@code batch} is empty
   */
  default Values batch(@SuppressWarnings("unchecked") T... batch) {
    return batch(Arrays.asList(batch));
  }

  /**
   * Creates a new extractor based on this one, but excluding the given
   * names.
   *
   * @param names an array of names to exclude, cannot be {@code null}
   * @return a new extractor minus the given names, never {@code null}
   * @throws IllegalArgumentException when one or more names are missing
   */
  default Extractor<T> excluding(String... names) {
    Set<String> set = new LinkedHashSet<>(List.of(names));
    Extractor<T> extractor = new DefaultExtractor<>(this.names().stream().map(n -> set.remove(n) ? "" : n).toList(), columnExtractor());

    if(!set.isEmpty()) {
      throw new IllegalArgumentException("unable to exclude non-existing fields: " + set + ", available are: " + this.names());
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
  default Extractor<T> only(String... names) {
    Set<String> set = new LinkedHashSet<>(List.of(names));
    Extractor<T> extractor = new DefaultExtractor<>(this.names().stream().map(n -> set.remove(n) ? n : "").toList(), columnExtractor());

    if(!set.isEmpty()) {
      throw new IllegalArgumentException("unable to keep non-existing fields: " + set);
    }

    return extractor;
  }
}
