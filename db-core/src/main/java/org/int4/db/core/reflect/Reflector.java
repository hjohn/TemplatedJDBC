package org.int4.db.core.reflect;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.int4.db.core.reflect.BeanClassDisecter.BeanClassDetails;
import org.int4.db.core.reflect.DefaultReflector.IndexedMapping;
import org.int4.db.core.reflect.RecordDisecter.RecordDetails;

/**
 * A combination of an {@link Extractor} and {@link Mapper} which can
 * extract values from a given type, as well as create new instances of it.
 *
 * @param <T> the reflected type
 */
public sealed interface Reflector<T> extends Extractor<T>, Mapper<T> permits DefaultReflector {

  /**
   * Creates a reflector for the given record type, mapping each of its
   * components (in order) to field names.
   *
   * @param <T> the record type
   * @param type a record type, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T extends Record> Reflector<T> of(Class<T> type) {
    return of(MethodHandles.publicLookup(), type);
  }

  /**
   * Creates a reflector for the given record type, mapping each of its
   * components (in order) to field names.
   *
   * @param <T> the record type
   * @param lookup a {@link Lookup} to access a non-public constructor, cannot be {@code null}
   * @param type a record type, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T extends Record> Reflector<T> of(Lookup lookup, Class<T> type) {
    RecordDetails<T> details = RecordDisecter.disect(Objects.requireNonNull(lookup, "lookup"), Objects.requireNonNull(type, "type"));

    return custom(type, details.creator(), details.mappings());
  }

  /**
   * Creates a reflector for conforming classes. To create a reflector, the class must conform to the following:
   *
   * <li>There must be an unambiguous accessible constructor with the most arguments, with a minimum of one argument</li>
   * <li>This constructor must have parameter names available</li>
   * <li>There must be corresponding accessible getter methods for each of the parameters of the chosen constructor with the correct name and type</li>
   *
   * Bean classes with an all arguments constructor generally conform to this contract. Make sure classes are compiled with parameter name information,
   * or use another way to create a reflector.
   *
   * @param <T> the type supported by the new reflector
   * @param type a class for which to create a corresponding reflector, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalArgumentException when the given class does not conform to the requirements
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T> Reflector<T> ofClass(Class<T> type) {
    return ofClass(MethodHandles.publicLookup(), type);
  }

  /**
   * Creates a reflector for conforming classes. To create a reflector, the class must conform to the following:
   *
   * <li>There must be an unambiguous accessible constructor with the most arguments, with a minimum of one argument</li>
   * <li>This constructor must have parameter names available</li>
   * <li>There must be corresponding accessible getter methods for each of the parameters of the chosen constructor with the correct name and type</li>
   *
   * Bean classes with an all arguments constructor generally conform to this contract. Make sure classes are compiled with parameter name information,
   * or use another way to create a reflector.
   *
   * @param <T> the type supported by the new reflector
   * @param lookup a {@link Lookup} to access non-public constructors and/or getters, cannot be {@code null}
   * @param type a class for which to create a corresponding reflector, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalArgumentException when the given class does not conform to the requirements
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T> Reflector<T> ofClass(Lookup lookup, Class<T> type) {
    BeanClassDetails<T> details = BeanClassDisecter.disect(Objects.requireNonNull(lookup, "lookup"), Objects.requireNonNull(type, "type"));

    return custom(type, details.creator(), details.mappings());
  }

  /**
   * Creates a fully customizable reflector.
   *
   * @param <T> the type of the custom type
   * @param type the type supported by the new reflector, cannot be {@code null}
   * @param mappings a list of {@link Mapping}s detailing how to extract data from the custom type, cannot be {@code null}, empty, or contain {@code null}s
   * @param creator a function to create a custom type from a given {@link Row}, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalArgumentException when mappings is empty, contains invalid identifiers or duplicates
   * @throws NullPointerException when any argument or list element is {@code null}
   */
  public static <T> Reflector<T> custom(Class<T> type, Function<Row, T> creator, List<Mapping<T, ?>> mappings) {
    if(Objects.requireNonNull(mappings, "mappings").isEmpty()) {
      throw new IllegalArgumentException("must specify at least one mapping");
    }

    List<IndexedMapping<T, Object>> indexedMappings = new ArrayList<>();
    int columnIndex = 0;

    for(int i = 0; i < mappings.size(); i++) {
      @SuppressWarnings("unchecked")
      Mapping<T, Object> mapping = Objects.requireNonNull((Mapping<T, Object>)mappings.get(i), "mappings[" + i + "]");

      indexedMappings.add(new IndexedMapping<>(columnIndex, mapping));
      columnIndex += mapping.columnCount();
    }

    return new DefaultReflector<>(type, Objects.requireNonNull(creator, "creator"), indexedMappings);
  }

  /**
   * Attempts to instantiate the type {@code T} given a {@link Row} and a column
   * offset within that row.
   *
   * @param row a {@link Row}, cannot be {@code null}
   * @param offset an offset, cannot be negative
   * @return an instance of type {@code T}, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  T instantiate(Row row, int offset);

  /**
   * Returns the type supported by this reflector.
   *
   * @return the type, never {@code null}
   */
  Class<T> getType();

  /**
   * Returns a new reflector with all field names prefixed with the given prefix.
   *
   * @param prefix a prefix, cannot be {@code null}
   * @return a new reflector, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   * @throws IllegalArgumentException when the prefix is empty or would result in illegal field names
   */
  Reflector<T> prefix(String prefix);

  /**
   * Returns a new reflector with the new given field names.
   *
   * @param fieldNames an array of new field names, cannot be {@code null}
   * @return a new reflector, never {@code null}
   * @throws IllegalArgumentException when the field names contain invalid identifiers or duplicates
   *   or the number of field names does not match the number of fields in the reflected type
   * @throws NullPointerException when any argument is {@code null}
   */
  Reflector<T> withNames(String... fieldNames);

  /**
   * Inlines the given reflector at the given field by name. All the fields of
   * the given reflector will become part of this reflector. If this would result
   * in duplicate names, then this operation fails.
   *
   * @param name a field to replace, cannot be {@code null}
   * @param reflector a {@link Reflector} to inline, cannot be {@code null}
   * @return a new reflector, never {@code null}
   * @throws IllegalArgumentException when the field to replace does not exist, is already
   *   inlined or is the result of another inlining, is of the incorrect type or the
   *   new configuration would have duplicate fields
   * @throws NullPointerException when any argument is {@code null}
   */
  Reflector<T> inline(String name, Reflector<?> reflector);

  /**
   * Inlines the given reflector at the given field by name. All the fields of
   * the reflector will become part of this reflector and will be prefixed with
   * the name followed by an underscore. If this would result in duplicate names,
   * then this operation fails, in which case manual renaming on the given
   * reflector (using {@link Reflector#prefix} or {@link Reflector#withNames} may
   * be required.
   *
   * @param name a field to replace, cannot be {@code null}
   * @param reflector a {@link Reflector} to inline, cannot be {@code null}
   * @return a new reflector, never {@code null}
   * @throws IllegalArgumentException when the field to replace does not exist, is already
   *   inlined or is the result of another inlining, is of the incorrect type or the
   *   new configuration would have duplicate fields
   * @throws NullPointerException when any argument is {@code null}
   */
  Reflector<T> nest(String name, Reflector<?> reflector);
}