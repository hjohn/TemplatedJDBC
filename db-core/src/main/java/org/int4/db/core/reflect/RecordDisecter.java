package org.int4.db.core.reflect;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

class RecordDisecter {
  record RecordDetails<T>(Function<Row, T> creator, List<Mapping<T, ?>> mappings) {}

  private static final Map<Class<?>, BiFunction<Row, Integer, ?>> MAP = Map.of(
    String.class, Row::getString,
    boolean.class, Row::getBoolean,
    int.class, Row::getInt,
    long.class, Row::getLong,
    double.class, Row::getDouble,
    byte[].class, Row::getBytes
  );

  static <T extends Record> RecordDetails<T> disect(Lookup lookup, Class<T> cls) {
    try {
      RecordComponent[] recordComponents = cls.getRecordComponents();
      Class<?>[] paramTypes = new Class<?>[recordComponents.length];
      List<Mapping<T, ?>> mappings = new ArrayList<>();

      for(int i = 0; i < recordComponents.length; i++) {
        RecordComponent component = recordComponents[i];

        @SuppressWarnings("unchecked")
        Class<Object> c = (Class<Object>)component.getType();
        MethodHandle handle = lookup.unreflect(component.getAccessor());

        paramTypes[i] = c;

        mappings.add(Mapping.of(NameTranslator.UNDERSCORED.toDatabaseName(component.getName()), c, handle::invoke));
      }

      MethodHandle constructor = lookup.unreflectConstructor(cls.getDeclaredConstructor(paramTypes)).asSpreader(Object[].class, paramTypes.length);

      return new RecordDetails<>(row -> buildRecord(cls, constructor, row), mappings);
    }
    catch(NoSuchMethodException | IllegalAccessException | SecurityException e) {
      throw new IllegalStateException("error accessing record's canonical constructor: " + cls, e);
    }
  }

  static <T> T buildRecord(Class<T> cls, MethodHandle constructor, Row row) {
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
}
