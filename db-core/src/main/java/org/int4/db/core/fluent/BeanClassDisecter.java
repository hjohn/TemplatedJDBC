package org.int4.db.core.fluent;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class BeanClassDisecter {
  record BeanClassDetails<T>(Function<Row, T> creator, List<Mapping<T, ?>> mappings) {}

  static <T> BeanClassDetails<T> disect(Lookup lookup, Class<T> cls) {
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
      List<Mapping<T, ?>> mappings = new ArrayList<>();
      Parameter[] parameters = constructor.getParameters();

      for(Parameter parameter : parameters) {
        if(!parameter.isNamePresent()) {
          throw new IllegalArgumentException("constructor " + constructor + " must have parameter name information for: " + cls);
        }

        try {
          MethodHandle extractor = lookup.findVirtual(cls, "get" + parameter.getName().substring(0, 1).toUpperCase() + parameter.getName().substring(1), MethodType.methodType(parameter.getType()));
          String databaseName = NameTranslator.UNDERSCORED.toDatabaseName(parameter.getName());
          @SuppressWarnings("unchecked")
          Class<Object> type = (Class<Object>)parameter.getType();

          mappings.add(Mapping.of(databaseName, type, extractor::invoke));
        }
        catch(IllegalAccessException e) {
          throw new IllegalArgumentException("getter for constructor parameter " + parameter.getName() + " in constructor " + constructor + " must be accessible via " + lookup + " for: " + cls);
        }
        catch(NoSuchMethodException e) {
          throw new IllegalArgumentException("unable to find getter for constructor parameter " + parameter.getName() + " for: " + cls);
        }
      }

      return new BeanClassDetails<>(
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
        mappings
      );
    }
    catch(IllegalAccessException e) {
      throw new IllegalArgumentException("constructor " + constructor + " must be accessible via " + lookup + " for: " + cls);
    }
  }
}
