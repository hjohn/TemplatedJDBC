package org.int4.db.core.reflect;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.int4.db.core.util.ColumnExtractor;

/**
 * Provides template parameters providing field and field/value
 * combinations.
 *
 * @param <T> the type being extracted from
 */
final class DefaultExtractor<T> implements Extractor<T> {
  private final List<String> names;
  private final ColumnExtractor<T> columnExtractor;

  DefaultExtractor(List<String> names, ColumnExtractor<T> columnExtractor) {
    this.names = List.copyOf(names);
    this.columnExtractor = columnExtractor;

    Set<String> uniqueNames = new HashSet<>();

    for(String name : names) {
      if(!name.isEmpty()) {
        if(!Identifier.isValidIdentifier(name)) {
          throw new IllegalArgumentException("names must only contain valid identifiers: " + name);
        }
        if(!uniqueNames.add(name)) {
          throw new IllegalArgumentException("names cannot contain duplicate names, but found duplicate: " + name + " in: " + names);
        }
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
  public String toString() {
    return names.toString();
  }
}
