package org.int4.db.core.fluent;

import java.util.Objects;

import org.int4.db.core.util.ThrowingFunction;

/**
 * Represents a mapping between an object and a field value.
 *
 * @param <T> the type of the object from which the field value is extracted
 * @param <F> the type of the field value
 */
public sealed interface Mapping<T, F> permits Mapping.Field, Mapping.Inline {

  /**
   * Creates a field mapping with the given parameters.
   *
   * @param <T> the type of the object from which the field value is extracted
   * @param <F> the type of the field value
   * @param name the name of the mapping, cannot be {@code null} and must be a valid identifier
   * @param type the class of the field value, cannot be {@code null}
   * @param extractor a function that extracts the field value from the given object, cannot be {@code null}
   * @return a new instance of Mapping, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T, F> Field<T, F> of(String name, Class<F> type, ThrowingFunction<T, F, Throwable> extractor) {
    return new Field<>(name, type, extractor);
  }

  /**
   * Creates an inline mapping with the given parameters.
   *
   * @param <T> the type of the object from which the field value is extracted
   * @param <F> the type of the field value
   * @param extractor a function that extracts the field value from the given object, cannot be {@code null}
   * @param reflector a reflector for the field value, cannot be {@code null}
   * @return a new instance of Mapping, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T, F> Inline<T, F> inline(ThrowingFunction<T, F, Throwable> extractor, Reflector<F> reflector) {
    return new Inline<>(extractor, reflector);
  }

  /**
   * Returns the class of the field value.
   *
   * @return the class of the field value, never {@code null}
   */
  Class<F> type();

  /**
   * Returns the extractor function that extracts the field value from the given object.
   *
   * @return the extractor function, never {@code null}
   */
  ThrowingFunction<T, F, Throwable> extractor();

  /**
   * Returns the number of columns mapped by this mapping.
   *
   * @return the number of columns, always positive
   */
  int columnCount();

  final class Field<T, F> implements Mapping<T, F> {
    private final String name;
    private final Class<F> type;
    private final ThrowingFunction<T, F, Throwable> extractor;

    Field(String name, Class<F> type, ThrowingFunction<T, F, Throwable> extractor) {
      this.name = Identifier.requireValidIdentifier(name, "name");
      this.type = Objects.requireNonNull(type, "type");
      this.extractor = Objects.requireNonNull(extractor, "extractor");
    }

    /**
     * Returns the name of the mapping.
     *
     * @return the name of the mapping, never {@code null}
     */
    public String name() {
      return name;
    }

    @Override
    public Class<F> type() {
      return type;
    }

    @Override
    public ThrowingFunction<T, F, Throwable> extractor() {
      return extractor;
    }

    @Override
    public int columnCount() {
      return 1;
    }
  }

  final class Inline<T, F> implements Mapping<T, F> {
    private final ThrowingFunction<T, F, Throwable> extractor;
    private final Reflector<F> reflector;

    Inline(ThrowingFunction<T, F, Throwable> extractor, Reflector<F> reflector) {
      this.extractor = Objects.requireNonNull(extractor, "extractor");
      this.reflector = Objects.requireNonNull(reflector, "reflector");
    }

    @Override
    public Class<F> type() {
      return reflector.getType();
    }

    @Override
    public ThrowingFunction<T, F, Throwable> extractor() {
      return extractor;
    }

    @Override
    public int columnCount() {
      return reflector.names().size();
    }

    /**
     * Returns the optional reflector for the field value.
     *
     * @return the reflector, never {@code null}
     */
    public Reflector<F> reflector() {
      return reflector;
    }
  }
}