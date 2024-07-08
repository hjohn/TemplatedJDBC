package org.int4.db.core.reflect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.int4.db.core.util.ColumnExtractor;
import org.int4.db.core.util.ThrowingFunction;

final class DefaultReflector<T> implements Reflector<T> {
  private final List<String> names;
  private final ColumnExtractor<T> columnExtractor;
  private final Class<T> type;
  private final Function<Row, T> creator;
  private final List<IndexedMapping<T, Object>> mappings;     // Tree, with same number of leafs as fields
  private final Map<Class<?>, TypeConverter<?, ?>> typeConverters;

  DefaultReflector(Class<T> type, Function<Row, T> creator, List<IndexedMapping<T, Object>> mappings, Map<Class<?>, TypeConverter<?, ?>> typeConverters) {
    this.names = extractNames(Objects.requireNonNull(mappings, "mappings"));
    this.columnExtractor = (obj, columnIndex) -> extract(obj, columnIndex);

    this.type = Objects.requireNonNull(type, "type");
    this.creator = Objects.requireNonNull(creator, "creator");
    this.mappings = List.copyOf(mappings);
    this.typeConverters = Map.copyOf(typeConverters);

    Set<String> uniqueNames = new HashSet<>();

    for(String name : names) {
      if(!uniqueNames.add(name)) {
        throw new IllegalArgumentException("names cannot contain duplicate names, but found duplicate: " + name + " in: " + names);
      }
    }
  }

  @Override
  public List<String> names() {
    return names;
  }

  @Override
  public ColumnExtractor<T> columnExtractor() {
    return columnExtractor;
  }

  @Override
  public T apply(Row row) {
    return instantiate(row, 0);
  }

  @Override
  public T instantiate(Row row, int offset) {
    return creator.apply(new RowAdapter(row, offset));
  }

  @Override
  public Class<T> getType() {
    return type;
  }

  @Override
  public Reflector<T> withNames(String... fieldNames) {
    if(fieldNames.length != names().size()) {
      throw new IllegalArgumentException("fieldNames must have length " + names().size());
    }

    List<IndexedMapping<T, Object>> newMappings = new ArrayList<>();
    int fieldIndex = 0;

    for(int i = 0; i < mappings.size(); i++) {
      IndexedMapping<T, Object> indexedMapping = mappings.get(i);
      Mapping<T, Object> mapping = indexedMapping.mapping();

      IndexedMapping<T, Object> newMapping = switch(mapping) {
        case Mapping.Field<T, Object> field -> new IndexedMapping<>(indexedMapping.columnIndex, Mapping.of(fieldNames[fieldIndex], field.type(), field.extractor()));
        case Mapping.Inline<T, Object> inline -> {
          String[] subNames = Arrays.copyOfRange(fieldNames, fieldIndex, fieldIndex + inline.reflector().names().size());
          Reflector<Object> renamedReflector = inline.reflector().withNames(subNames);

          yield new IndexedMapping<>(indexedMapping.columnIndex, Mapping.inline(mapping.extractor(), renamedReflector));
        }
      };

      newMappings.add(newMapping);
      fieldIndex += newMapping.columnCount();
    }

    return new DefaultReflector<>(type, creator, newMappings, typeConverters);
  }

  @Override
  public Reflector<T> inline(String name, Reflector<?> reflector) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(reflector, "reflector");

    return insertReflector(name, reflector);
  }

  @Override
  public Reflector<T> nest(String name, Reflector<?> reflector) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(reflector, "reflector");

    return insertReflector(name, reflector.prefix(name + "_"));
  }

  @Override
  public Reflector<T> prefix(String prefix) {
    return new DefaultReflector<>(type, creator, mappings.stream().map(m -> m.prefix(prefix)).toList(), typeConverters);
  }

  private static <T, F> int columnIndexToMappingIndex(List<IndexedMapping<T, F>> mappings, int columnIndex) {
    for(int i = mappings.size(); i-- > 0; ) {
      IndexedMapping<?, ?> mapping = mappings.get(i);

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

    if(mappingIndex == -1 || mappings.get(mappingIndex).mapping instanceof Mapping.Inline) {
      List<String> validNames = mappings.stream().filter(m -> m.mapping instanceof Mapping.Field).map(m -> names().get(m.columnIndex)).toList();

      throw new IllegalArgumentException("can't inline field with name '" + reflectorName + "'; must be one of: " + validNames);
    }

    IndexedMapping<T, Object> mapping = mappings.get(mappingIndex);

    if(!mapping.type().equals(subReflector.getType())) {
      throw new IllegalArgumentException("reflector for inlining field '" + reflectorName + "' must be of type: " + mapping.type());
    }

    @SuppressWarnings("unchecked")
    Reflector<Object> castSubReflector = (Reflector<Object>)subReflector;
    List<IndexedMapping<T, Object>> newMappings = new ArrayList<>();
    int shift = subReflector.names().size() - 1;

    newMappings.addAll(mappings.subList(0, mappingIndex));
    newMappings.add(new IndexedMapping<>(index, Mapping.inline(mapping.extractor(), castSubReflector)));
    newMappings.addAll(mappings.subList(mappingIndex + 1, mappings.size()).stream().map(m -> new IndexedMapping<>(m.columnIndex + shift, m.mapping)).toList());

    return new DefaultReflector<>(type, creator, newMappings, typeConverters);
  }

  @Override
  public <V> Reflector<T> addTypeConverter(Class<V> javaType, TypeConverter<V, ?> typeConverter) {
    Map<Class<?>, TypeConverter<?, ?>> map = new HashMap<>(typeConverters);

    map.put(javaType, typeConverter);

    return new DefaultReflector<>(type, creator, mappings, map);
  }

  private static <T, F> List<String> extractNames(List<IndexedMapping<T, F>> mappings) {
    return mappings.stream().flatMap(m -> m.names().stream()).toList();
  }

  private Object extract(T obj, int columnIndex) {
    try {
      IndexedMapping<T, Object> mapping = mappings.get(columnIndexToMappingIndex(mappings, columnIndex));
      Object result = mapping.extractor().apply(obj);

      if(mapping.mapping instanceof Mapping.Inline<T, Object> i) {
        return i.reflector().columnExtractor().extract(result, columnIndex - mapping.columnIndex);
      }

      if(result == null) {
        return null;
      }

      @SuppressWarnings("unchecked")
      TypeConverter<Object, T> typeConverter = (TypeConverter<Object, T>)typeConverters.get(mapping.type());

      if(typeConverter != null) {
        return typeConverter.encode(result);
      }

      return result;
    }
    catch(Throwable e) {
      throw new IllegalStateException("Unable to access component " + columnIndex + " of " + obj, e);
    }
  }

  record IndexedMapping<T, F>(int columnIndex, Mapping<T, F> mapping) {

    int columnCount() {
      return mapping.columnCount();
    }

    List<String> names() {
      return switch(mapping) {
        case Mapping.Field<T, F> f -> List.of(f.name());
        case Mapping.Inline<T, F> i -> i.reflector().names();
      };
    }

    Class<F> type() {
      return mapping.type();
    }

    ThrowingFunction<T, F, Throwable> extractor() {
      return mapping.extractor();
    }

    IndexedMapping<T, F> prefix(String prefix) {
      return switch(mapping) {
        case Mapping.Field<T, F> f -> new IndexedMapping<>(columnIndex, Mapping.of(prefix + f.name(), type(), extractor()));
        case Mapping.Inline<T, F> i -> new IndexedMapping<>(columnIndex, Mapping.inline(extractor(), i.reflector().prefix(prefix)));
      };
    }
  }

  // TODO RowAdapters wrap every Row, but if the Row instance is the same (which happens with DynamicRow) these could be cached
  private class RowAdapter implements Row {
    private final Row row;
    private final int offset;

    RowAdapter(Row row, int offset) {
      this.row = row;
      this.offset = offset;
    }

    private int map(int columnIndex) {
      Objects.checkIndex(columnIndex, mappings.size());

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
      return getObject(columnIndex, Object.class);
    }

    @Override
    public <F> F getObject(int columnIndex, Class<F> type) {
      IndexedMapping<T, ?> indexedMapping = mappings.get(columnIndex);

      if(indexedMapping.mapping instanceof Mapping.Inline i) {
        @SuppressWarnings("unchecked")
        F t = (F)i.reflector().instantiate(row, offset + indexedMapping.columnIndex);

        return t;
      }

      @SuppressWarnings("unchecked")
      TypeConverter<F, Object> typeConverter = (TypeConverter<F, Object>)typeConverters.get(indexedMapping.type());

      if(typeConverter != null) {
        Object result = row.getObject(map(columnIndex));

        return result == null ? null : typeConverter.decode(result);
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
}
