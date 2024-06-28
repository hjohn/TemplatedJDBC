package org.int4.db.core.api;

import java.util.Objects;
import java.util.function.Function;

/**
 * Responsible for converting types from a JDBC type to a Java type, encoding to SQL types and
 * decoding to Java types.
 *
 * @param <V> the decoded java type
 * @param <E> the encoded JDBC type
 */
public interface TypeConverter<V, E> {

  /**
   * Creates a new converter that uses the given functions to perform the encoding and
   * decoding conversions.
   *
   * @param <V> the decoded java type
   * @param <E> the encoded JDBC type
   * @param encodedClass the encoded JDBC type, cannot be {@code null}
   * @param encoder an encoder, cannot be {@code null}
   * @param decoder a decoder, cannot be {@code null}
   * @return a {@link TypeConverter}, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  static <V, E> TypeConverter<V, E> of(Class<E> encodedClass, Function<V, E> encoder, Function<E, V> decoder) {
    Objects.requireNonNull(encodedClass, "encodedClass");
    Objects.requireNonNull(encoder, "encoder");
    Objects.requireNonNull(decoder, "decoder");

    return new TypeConverter<>() {
      @Override
      public Class<E> encodedClass() {
        return encodedClass;
      }

      @Override
      public E encode(V value) {
        return encoder.apply(value);
      }

      @Override
      public V decode(E encodedValue) {
        return decoder.apply(encodedValue);
      }
    };
  }

  /**
   * A supported JDBC type to encode to.
   *
   * @return a supported JDBC type to encode to, never {@code null}
   */
  Class<E> encodedClass();

  /**
   * Encodes a Java type to a JDBC type.
   *
   * @param value a value of the Java type, cannot be {@code null}
   * @return a value of the JDBC type, never {@code null}
   */
  E encode(V value);

  /**
   * Decodes a JDBC type to a Java type.
   *
   * @param encodedValue a value of the JDBC type, cannot be {@code null}
   * @return a value of the Java type, never {@code null}
   */
  V decode(E encodedValue);

}
