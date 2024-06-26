package org.int4.db.core.fluent;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.int4.db.core.util.ThrowingFunction;

/**
 * A combination of an {@link Extractor} and {@link Mapper} which can
 * extract values from a given type, as well as create new instances of it.
 *
 * @param <T> the reflected type
 */
public class Reflector<T> extends Extractor<T> implements Mapper<T> {

  /**
   * Creates a reflector for the given record type, mapping each of its
   * components (in order) to field names.
   *
   * @param <T> the record type
   * @param baseType a record type, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T extends Record> Reflector<T> of(Class<T> baseType) {
    return of(MethodHandles.publicLookup(), baseType);
  }

  /**
   * Creates a reflector for the given record type, mapping each of its
   * components (in order) to field names.
   *
   * @param <T> the record type
   * @param lookup a {@link Lookup} to access a non-public constructor, cannot be {@code null}
   * @param baseType a record type, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T extends Record> Reflector<T> of(Lookup lookup, Class<T> baseType) {
    RecordDetails<T> details = disectRecord(Objects.requireNonNull(lookup, "lookup"), Objects.requireNonNull(baseType, "baseType"));
    List<Mapping> mappings = IntStream.range(0, details.names.size()).mapToObj(i -> new Mapping(i, null, details.types.get(i))).toList();

    return new Reflector<>(
      baseType,
      details.names,
      row -> buildRecord(baseType, details.constructor, row),
      (t, index) -> {
        try {
          return details.extractors.get(index).apply(t);
        }
        catch(Throwable e) {
          throw new IllegalStateException("Unable to access component " + index + " of " + t, e);
        }
      },
      mappings,
      null
    );
  }

  /**
   * Creates a fully customizable reflector.
   *
   * @param <T> the type of the custom type
   * @param type the type supported by the new reflector, cannot be {@code null}
   * @param fieldNames a list of field names the custom type maps to, cannot be {@code null}, empty, or contain {@code null}s
   * @param fieldTypes a list of field types the custom type maps to, cannot be {@code null}, empty, or contain {@code null}s
   * @param creator a function to create a custom type from a given {@link Row}, cannot be {@code null}
   * @param dataExtractor a function to extract a field by index (zero based) from the custom type, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalArgumentException when field names is empty, contains invalid identifiers or duplicates,
   *   and when the names and types list have different lengths
   * @throws NullPointerException when any argument or list element is {@code null}
   */
  public static <T> Reflector<T> custom(Class<T> type, List<String> fieldNames, List<Class<?>> fieldTypes, Function<Row, T> creator, BiFunction<T, Integer, Object> dataExtractor) {
    if(Objects.requireNonNull(fieldNames, "fieldNames").isEmpty()) {
      throw new IllegalArgumentException("must specify at least one field name");
    }

    if(fieldNames.size() != Objects.requireNonNull(fieldTypes, "fieldTypes").size()) {
      throw new IllegalArgumentException("must specify an equal number of names and types");
    }

    fieldNames.stream().filter(Predicate.not(Identifier::isValidIdentifier)).findFirst().ifPresent(name -> {
      throw new IllegalArgumentException("invalid identifier found in field names: " + name);
    });

    if(fieldNames.stream().distinct().count() != fieldNames.size()) {
      throw new IllegalArgumentException("duplicate identifiers found in field names: " + fieldNames);
    }

    List<Mapping> mappings = new ArrayList<>();

    for(int i = 0; i < fieldTypes.size(); i++) {
      mappings.add(new Mapping(i, null, Objects.requireNonNull(fieldTypes.get(i), "fieldTypes[" + i + "]")));
    }

    return new Reflector<>(type, fieldNames, Objects.requireNonNull(creator, "creator"), Objects.requireNonNull(dataExtractor, "dataExtractor"), mappings, null);
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
   * @param cls a class for which to create a corresponding reflector, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalArgumentException when the given class does not conform to the requirements
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T> Reflector<T> ofClass(Class<T> cls) {
    return ofClass(MethodHandles.publicLookup(), cls);
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
   * @param cls a class for which to create a corresponding reflector, cannot be {@code null}
   * @return a reflector, never {@code null}
   * @throws IllegalArgumentException when the given class does not conform to the requirements
   * @throws NullPointerException when any argument is {@code null}
   */
  public static <T> Reflector<T> ofClass(Lookup lookup, Class<T> cls) {
    List<Constructor<?>> bestConstructors = new ArrayList<>();
    int maxParameters = 1;

    for(Constructor<?> constructor : cls.getDeclaredConstructors()) {
      if(constructor.getParameterCount() > maxParameters) {
        bestConstructors.clear();
        bestConstructors.add(constructor);
        maxParameters = constructor.getParameterCount();
      }
      else if(constructor.getParameterCount() == maxParameters) {
        bestConstructors.add(constructor);
      }
    }

    if(bestConstructors.size() == 0) {
      throw new IllegalArgumentException("must have at least one constructor with one or more arguments: " + cls);
    }
    if(bestConstructors.size() > 1) {
      throw new IllegalArgumentException("must have a single constructor with the highest number or arguments, but found multiple: " + cls);
    }

    Constructor<?> constructor = bestConstructors.get(0);

    try {
      MethodHandle mh = lookup.unreflectConstructor(constructor).asSpreader(Object[].class, maxParameters);
      List<String> names = new ArrayList<>();
      List<Mapping> mappings = new ArrayList<>();
      Map<Integer, ThrowingFunction<T, Object, Throwable>> extractors = new HashMap<>();
      Parameter[] parameters = constructor.getParameters();

      for(int i = 0; i < parameters.length; i++) {
        Parameter p = parameters[i];

        if(!p.isNamePresent()) {
          throw new IllegalArgumentException("constructor " + constructor + " must have parameter name information for: " + cls);
        }

        try {
          MethodHandle extractor = lookup.findVirtual(cls, "get" + p.getName().substring(0, 1).toUpperCase() + p.getName().substring(1), MethodType.methodType(p.getType()));

          names.add(NameTranslator.UNDERSCORED.toDatabaseName(p.getName()));
          mappings.add(new Mapping(i, null, p.getType()));
          extractors.put(i, o -> extractor.invoke(o));
        }
        catch(IllegalAccessException e) {
          throw new IllegalArgumentException("getter for constructor parameter " + p.getName() + " in constructor " + constructor + " must be accessible via " + lookup + " for: " + cls);
        }
        catch(NoSuchMethodException e) {
          throw new IllegalArgumentException("unable to find getter for constructor parameter " + p.getName() + " for: " + cls);
        }
      }

      return new Reflector<>(
        cls,
        names,
        row -> {
          Object[] args = new Object[row.getColumnCount()];

          for(int i = 0; i < args.length; i++) {
            args[i] = row.getObject(i);
          }

          try {
            return (T)mh.invoke(args);
          }
          catch(Throwable e) {
            throw new IllegalStateException("unable to access constructor " + constructor, e);
          }
        },
        (t, index) -> {
          try {
            return extractors.get(index).apply(t);
          }
          catch(Throwable e) {
            throw new IllegalStateException("unable to access component " + index + " of " + t, e);
          }
        },
        mappings,
        null
      );
    }
    catch(IllegalAccessException e) {
      throw new IllegalArgumentException("constructor " + constructor + " must be accessible via " + lookup + " for: " + cls);
    }
  }

  private final Class<T> type;
  private final Function<Row, T> creator;
  private final Reflector<T> root;
  private final List<Mapping> mappings;

  Reflector(Class<T> type, List<String> names, Function<Row, T> creator, BiFunction<T, Integer, Object> dataExtractor, List<Mapping> mappings, Reflector<T> root) {
    super(names, dataExtractor);

    this.type = Objects.requireNonNull(type, "type");
    this.creator = Objects.requireNonNull(creator, "creator");
    this.mappings = Objects.requireNonNull(mappings, "mappings"); //== null ? IntStream.range(0, names.size()).mapToObj(i -> new Mapping(i, null)).toList() : mappings;
    this.root = root;
  }

  /**
   * Sets the field names to use.
   *
   * @param fieldNames a list of field names to map the type to, cannot be {@code null}
   * @return a new reflector, never {@code null}
   * @throws IllegalArgumentException when the field names contain invalid identifiers or duplicates
   *   or the number of field names does not match the number of fields in the reflected type
   */
  public Reflector<T> withNames(String... fieldNames) {
    if(fieldNames.length != names().size()) {
      throw new IllegalArgumentException("fieldNames must have length " + names().size());
    }

    return new Reflector<>(type, Arrays.asList(fieldNames), creator, dataExtractor, mappings, root == null ? this : root);
  }

  /**
   * Inlines the given reflector at the given field by name. All the fields of
   * the reflector will become part of this reflector and prefixed with the name.
   *
   * @param name a field to replace, cannot be {@code null}
   * @param reflector a {@link Reflector} to inline, cannot be {@code null}
   * @return a new reflector, never {@code null}
   * @throws IllegalArgumentException when the field to replace does not exist, is already
   *   inlined or is the result of another inlining, or is of the incorrect type
   * @throws NullPointerException when any argument is {@code null}
   */
  public Reflector<T> inline(String name, Reflector<?> reflector) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(reflector, "reflector");

    return insertReflector(name, reflector);
  }

  private BiFunction<T, Integer, Object> getRootDataExtractor() {
    return root == null ? dataExtractor : root.dataExtractor;
  }

  private Function<Row, T> getRootCreator() {
    return root == null ? creator : root.creator;
  }

  private static int columnIndexToMappingIndex(List<Mapping> mappings, int columnIndex) {
    for(int i = mappings.size(); i-- > 0; ) {
      Mapping mapping = mappings.get(i);

      if(mapping.columnIndex <= columnIndex) {
        return i;
      }
    }

    throw new AssertionError("could not find a mapping with columnIndex " + columnIndex + ": " + mappings);
  }

  private Reflector<T> insertReflector(String reflectorName, Reflector<?> subReflector) {
    List<String> names = names();
    int index = names.indexOf(reflectorName);
    int mappingIndex = index == -1 ? -1 : columnIndexToMappingIndex(mappings, index);

    if(mappingIndex == -1 || mappings.get(mappingIndex).reflector != null) {
      List<String> validNames = mappings.stream().filter(m -> m.reflector == null).map(m -> names().get(m.columnIndex)).toList();

      throw new IllegalArgumentException("can't inline field with name '" + reflectorName + "'; must be one of: " + validNames);
    }

    Mapping mapping = mappings.get(mappingIndex);

    if(!mapping.type.equals(subReflector.type)) {
      throw new IllegalArgumentException("reflector for inlining field '" + reflectorName + "' must be of type: " + mapping.type);
    }

    List<String> newNames = new ArrayList<>();

    newNames.addAll(names.subList(0, index));
    newNames.addAll(subReflector.names().stream().map(n -> reflectorName + "_" + n).toList());
    newNames.addAll(names.subList(index + 1, names.size()));

    List<Mapping> newMappings = new ArrayList<>();
    int shift = subReflector.names().size() - 1;

    newMappings.addAll(mappings.subList(0, mappingIndex));
    newMappings.add(new Mapping(index, subReflector, mapping.type));
    newMappings.addAll(mappings.subList(mappingIndex + 1, mappings.size()).stream().map(m -> new Mapping(m.columnIndex + shift, m.reflector, m.type)).toList());

    return new Reflector<>(
      type,
      newNames,
      row -> getRootCreator().apply(new RowAdapter(row, newMappings, 0)),
      wrapRootDataExtractor(newMappings),
      newMappings,
      root == null ? this : root
    );
  }

  private BiFunction<T, Integer, Object> wrapRootDataExtractor(List<Mapping> mappings) {
    return (t, columnIndex) -> {
      int mappingIndex = columnIndexToMappingIndex(mappings, columnIndex);
      Mapping mapping = mappings.get(mappingIndex);
      Object obj = getRootDataExtractor().apply(t, mappingIndex);

      if(mapping.reflector == null) {
        return obj;
      }

      @SuppressWarnings("unchecked")
      BiFunction<Object, Integer, Object> subDataExtractor = (BiFunction<Object, Integer, Object>)mapping.reflector.dataExtractor;

      return subDataExtractor.apply(obj, columnIndex - mapping.columnIndex);
    };
  }

  private record Mapping(int columnIndex, Reflector<?> reflector, Class<?> type) {
    Object createObject(Row row, int offset) {
      return reflector.getRootCreator().apply(new RowAdapter(row, reflector.mappings, offset));
    }
  }

  private static class RowAdapter implements Row {
    private final List<Mapping> mappings;
    private final Row row;
    private final int offset;

    RowAdapter(Row row, List<Mapping> mappings, int offset) {
      this.row = row;
      this.mappings = mappings;
      this.offset = offset;
    }

    private int map(int columnIndex) {
      return mappings.get(columnIndex).columnIndex + offset;
    }

    @Override
    public int getColumnCount() {
      return mappings.size();
    }

    @Override
    public byte[] getBytes(int columnIndex) {
      return row.getBytes(map(columnIndex));
    }

    @Override
    public String getString(int columnIndex) {
      return row.getString(map(columnIndex));
    }

    @Override
    public Object getObject(int columnIndex) {
      Mapping mapping = mappings.get(columnIndex);

      if(mapping.reflector != null) {
        return mapping.createObject(row, offset + mapping.columnIndex);
      }

      return row.getObject(map(columnIndex));
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
      Mapping mapping = mappings.get(columnIndex);

      if(mapping.reflector != null) {
        @SuppressWarnings("unchecked")
        T t = (T)mapping.createObject(row, offset + mapping.columnIndex);

        return t;
      }

      return row.getObject(map(columnIndex), type);
    }

    @Override
    public double getDouble(int columnIndex) {
      return row.getDouble(map(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) {
      return row.getLong(map(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) {
      return row.getInt(map(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      return row.getBoolean(map(columnIndex));
    }

    @Override
    public String toString() {
      return "RowAdapter[" + row.toString() + ", mappings = " + mappings + "]";
    }
  }

  @Override
  public T apply(Row row) {
    return creator.apply(row);
  }

  private static final Map<Class<?>, BiFunction<Row, Integer, ?>> MAP = Map.of(
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

  private static <T> T buildRecord(Class<T> cls, MethodHandle constructor, Row row) {
    RecordComponent[] recordComponents = cls.getRecordComponents();
    Object[] args = new Object[recordComponents.length];

    for(int i = 0; i < recordComponents.length; i++) {
      RecordComponent rc = recordComponents[i];
      BiFunction<Row, Integer, ?> valueExtractor = MAP.get(rc.getType());

      if(valueExtractor == null) {
        args[i] = row.getObject(i, rc.getType());
      }
      else {
        args[i] = valueExtractor.apply(row, i);
      }
    }

    try {
      return (T)constructor.invoke(args);
    }
    catch(Throwable t) {
      throw new IllegalStateException("construction failed for record of type: " + cls, t);
    }
  }

  private record RecordDetails<T>(MethodHandle constructor, Map<Integer, ThrowingFunction<T, Object, Throwable>> extractors, List<String> names, List<Class<?>> types) {}

  private static <T extends Record> RecordDetails<T> disectRecord(Lookup lookup, Class<T> cls) {
    List<String> names = new ArrayList<>();
    Map<Integer, ThrowingFunction<T, Object, Throwable>> extractors = new HashMap<>();

    try {
      RecordComponent[] recordComponents = cls.getRecordComponents();
      Class<?>[] paramTypes = new Class<?>[recordComponents.length];

      for(int i = 0; i < recordComponents.length; i++) {
        RecordComponent component = recordComponents[i];
        Class<?> c = component.getType();
        MethodHandle handle = lookup.unreflect(component.getAccessor());

        paramTypes[i] = c;

        names.add(NameTranslator.UNDERSCORED.toDatabaseName(component.getName()));
        extractors.put(i, handle::invoke);
      }

      MethodHandle constructor = lookup.unreflectConstructor(cls.getDeclaredConstructor(paramTypes)).asSpreader(Object[].class, paramTypes.length);

      return new RecordDetails<>(constructor, extractors, names, Arrays.asList(paramTypes));
    }
    catch(NoSuchMethodException | IllegalAccessException | SecurityException e) {
      throw new IllegalStateException("error accessing record's canonical constructor: " + cls, e);
    }
  }
}