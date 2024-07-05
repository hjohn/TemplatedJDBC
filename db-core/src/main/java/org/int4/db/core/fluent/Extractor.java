package org.int4.db.core.fluent;

import java.util.List;

import org.int4.db.core.fluent.FieldValueSetParameter.Entries;
import org.int4.db.core.fluent.FieldValueSetParameter.Values;

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
   * Given a type {@code T}, provides a template parameter that inserts its fields
   * comma separated in the form of "field = value" suitable for UPDATE statements.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @return an entries template parameter, never {@code null}
   */
  Entries entries(T t);

  /**
   * Given a type {@code T}, provides a template parameter that inserts its values
   * comma separated, suitable for the INSERT statement's VALUES clause.
   *
   * @param t a type {@code T}, cannot be {@code null}
   * @return a values template parameter, never {@code null}
   */
  Values values(T t);

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
  Values batch(List<T> batch);

  /**
   * Creates a new extractor based on this one, but excluding the given
   * names.
   *
   * @param names an array of names to exclude, cannot be {@code null}
   * @return a new extractor minus the given names, never {@code null}
   * @throws IllegalArgumentException when one or more names are missing
   */
  Extractor<T> excluding(String... names);

  /**
   * Creates a new extractor based on this one, but keeping only the
   * given names.
   *
   * @param names an array of names to keep, cannot be {@code null}
   * @return a new extractor with only the given names, never {@code null}
   * @throws IllegalArgumentException when one or more names are missing
   */
  Extractor<T> only(String... names);

  /**
   * Extracts the object at the given column index from the given type
   * {@code T}.
   *
   * @param t an instance of type {@code T}, cannot be {@code null}
   * @param columnIndex a column index, cannot be negative
   * @return the object at the given column index, can be {@code null}
   */
  Object extractObject(T t, int columnIndex);
}
