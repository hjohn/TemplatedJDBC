package org.int4.db.core.fluent;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.RecordComponent;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.int4.db.core.util.JdbcFunction;
import org.int4.db.core.util.ThrowingFunction;

public class Reflector<T> extends Extractor<T> implements Mapper<T> {

  /**
   * Creates a reflector for the given record type, mapping each of its
   * components (in order) to the given (database field) names.
   *
   * @param baseType a record type, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalStateException when the number of names given does not match the number of components in the given type (and its sub types)
   */
  public static <T extends Record> Reflector<T> of(Class<T> baseType) {
    return of(MethodHandles.publicLookup(), baseType);
  }

  public static <T extends Record> Reflector<T> of(Lookup lookup, Class<T> baseType) {
    ConstructorsBuilder builder = new ConstructorsBuilder(lookup, baseType);

    return new Reflector<>(
      builder.names,
      row -> new RecordBuilder(row, builder.constructors.iterator()).build(baseType),
      (t, index) -> {
        try {
          return builder.extractors.get(index).apply(t);
        }
        catch(Throwable e) {
          throw new IllegalStateException("Unable to access component " + index + " of " + t, e);
        }
      }
    );
  }

  public static <T> Reflector<T> custom(List<String> fieldNames, JdbcFunction<Row, T> creator, BiFunction<T, Integer, Object> dataExtractor) {
    return new Reflector<>(fieldNames, creator, dataExtractor);
  }

  private final JdbcFunction<Row, T> creator;

  Reflector(List<String> names, JdbcFunction<Row, T> creator, BiFunction<T, Integer, Object> dataExtractor) {
    super(names, dataExtractor);

    this.creator = creator;
  }

  public Reflector<T> withNames(String... fieldNames) {
    if(fieldNames.length != names().size()) {
      throw new IllegalArgumentException("fieldNames must have length " + names().size());
    }

    return new Reflector<>(Arrays.asList(fieldNames), creator, dataExtractor);
  }

  @Override
  public T apply(Row row) throws SQLException {
    return creator.apply(row);
  }

  interface ThrowingBiFunction<T, U, R> {
    R apply(T t, U u) throws SQLException;
  }

  private static final Map<Class<?>, ThrowingBiFunction<Row, Integer, ?>> MAP = Map.of(
    String.class, Row::getString,
    Boolean.class, Row::getBoolean,
    Integer.class, Row::getInt,
    Long.class, Row::getLong,
    Double.class, Row::getDouble,
    boolean.class, Row::getBoolean,
    int.class, Row::getInt,
    long.class, Row::getLong,
    double.class, Row::getDouble,
    byte[].class, Row::getBytes
  );

  private static class RecordBuilder {
    private final Row row;
    private final Iterator<MethodHandle> iterator;

    private int columnIndex;

    RecordBuilder(Row row, Iterator<MethodHandle> iterator) {
      this.row = row;
      this.iterator = iterator;
    }

    private <T> T build(Class<T> cls) throws SQLException {
      RecordComponent[] recordComponents = cls.getRecordComponents();
      Object[] args = new Object[recordComponents.length];

      for(int i = 0; i < recordComponents.length; i++) {
        RecordComponent rc = recordComponents[i];

        if(rc.getType().isRecord()) {  // nested record?
          args[i] = build(rc.getType());
        }
        else {
          ThrowingBiFunction<Row, Integer, ?> valueExtractor = MAP.get(rc.getType());

          if(valueExtractor == null) {
            args[i] = row.getObject(columnIndex++, rc.getType());
          }
          else {
            args[i] = valueExtractor.apply(row, columnIndex++);
          }
        }
      }

      try {
        return (T)iterator.next().invoke(args);
      }
      catch(Throwable t) {
        throw new IllegalStateException("construction failed for record of type: " + cls, t);
      }
    }
  }

  private static class ConstructorsBuilder {
    private static final Pattern PATTERN = Pattern.compile("(.*?)(?:_([0-9]+))");

    private final Lookup lookup;
    private final List<String> names = new ArrayList<>();
    private final List<MethodHandle> constructors = new ArrayList<>();
    private final Map<Integer, ThrowingFunction<Object, Object, Throwable>> extractors = new HashMap<>();

    private int componentIndex;

    ConstructorsBuilder(Lookup lookup, Class<? extends Record> baseClass) {
      this.lookup = lookup;

      build(baseClass, x -> x);
      renameDuplicates();
    }

    private void renameDuplicates() {
      for(int i = 1; i < names.size(); i++) {
        String originalName = names.get(i);
        String candidateName = originalName;
        Matcher matcher = PATTERN.matcher(candidateName);
        String baseName = matcher.matches() ? matcher.group(1) : candidateName;
        int count = matcher.matches() ? Integer.parseInt(matcher.group(2)) : 2;

        retest:
        for(;;) {
          for(int j = 0; j < names.size(); j++) {
            if(j != i) {
              if(names.get(j).equals(candidateName) && (j < i || !originalName.equals(candidateName))) {
                candidateName = baseName + "_" + count++;
                continue retest;
              }
            }
          }

          break;
        }

        names.set(i, candidateName);
      }
    }

    private void build(Class<? extends Record> cls, ThrowingFunction<Object, Object, Throwable> base) {
      try {
        RecordComponent[] recordComponents = cls.getRecordComponents();
        Class<?>[] paramTypes = new Class<?>[recordComponents.length];

        for(int i = 0; i < recordComponents.length; i++) {
          RecordComponent component = recordComponents[i];
          Class<?> c = component.getType();
          MethodHandle handle = lookup.unreflect(component.getAccessor()).asType(MethodType.methodType(Object.class, Object.class));

          paramTypes[i] = c;

          if(c.isRecord()) {
            @SuppressWarnings("unchecked")
            Class<? extends Record> recordClass = (Class<? extends Record>)c;

            build(recordClass, base.andThen(o -> handle.invokeExact(o)));
          }
          else {
            names.add(NameTranslator.UNDERSCORED.toDatabaseName(component.getName()));
            extractors.put(componentIndex, base.andThen(o -> handle.invokeExact(o)));

            componentIndex++;
          }
        }

        constructors.add(lookup.unreflectConstructor(cls.getDeclaredConstructor(paramTypes)).asSpreader(Object[].class, paramTypes.length));
      }
      catch(NoSuchMethodException | IllegalAccessException | SecurityException e) {
        throw new IllegalStateException("error accessing record's canonical constructor: " + cls);
      }
    }
  }
}