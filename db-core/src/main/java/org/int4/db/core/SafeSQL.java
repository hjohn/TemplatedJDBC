package org.int4.db.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.int4.db.core.fluent.Extractor;
import org.int4.db.core.fluent.Identifier;
import org.int4.db.core.fluent.FieldValueSetParameter.Entries;
import org.int4.db.core.fluent.FieldValueSetParameter.Values;

class SafeSQL {
  private static final Predicate<String> NOT_EMPTY = Predicate.not(String::isEmpty);
  private static final Pattern ALIAS = Pattern.compile(".*? (([a-zA-Z][a-zA-Z_0-9]*) *\\. *)");

  private final StringTemplate template;

  SafeSQL(StringTemplate template) {
    this.template = template;
  }

  @Override
  public String toString() {
    return template.toString();
  }

  PreparedStatement toPreparedStatement(Connection connection) throws SQLException {
    StringBuilder sb = new StringBuilder();
    List<String> fragments = template.fragments();
    List<Object> values = template.values();

    for(int i = 0; i < values.size(); i++) {
      String fragment = fragments.get(i);
      Object value = values.get(i);

      sb.append(fragment);

      if(value instanceof Extractor<?> r) {
        Matcher matcher = ALIAS.matcher(fragment);
        String alias = matcher.matches() ? matcher.group(2) + "." : "";

        if(!alias.isEmpty()) {
          sb.delete(sb.length() - matcher.group(1).length(), sb.length());
        }

        sb.append(r.names().stream().filter(NOT_EMPTY).map(n -> alias + n).collect(Collectors.joining(", ")));
      }
      else if(value instanceof Entries e) {
        sb.append(e.names().stream().filter(NOT_EMPTY).map(t -> t + " = ?").collect(Collectors.joining(", ")));
      }
      else if(value instanceof Values v) {
        sb.append(v.names().stream().filter(NOT_EMPTY).map(t -> "?").collect(Collectors.joining(", ")));
      }
      else if(value instanceof Identifier id) {
        sb.append(id.getIdentifier());
      }
      else if(value instanceof Record r) {
        RecordComponent[] recordComponents = r.getClass().getRecordComponents();

        for (int j = 0; j < recordComponents.length; j++) {
          if (j != 0) {
            sb.append(",");
          }

          sb.append("?");
        }
      }
      else {
        sb.append("?");
      }
    }

    sb.append(fragments.getLast());

    PreparedStatement ps = connection.prepareStatement(sb.toString(), Statement.RETURN_GENERATED_KEYS);

    fillParameters(ps, template.values());

    return ps;
  }

  private static void fillParameters(PreparedStatement ps, List<Object> values) throws SQLException {
    int index = 1;

    for(Object value : values) {
      index = fillParameter(index, ps, value);
    }
  }

  private static int fillParameter(int startIndex, PreparedStatement ps, Object value) throws SQLException {
    int index = startIndex;

    switch(value) {
      case Entries e -> {
        for(int i = 0, max = e.size(); i < max; i++) {
          String name = e.getName(i);

          if(!name.isEmpty()) {
            index = fillParameter(index, ps, e.getValue(i));
          }
        }
      }
      case Values v -> {
        for(int i = 0, max = v.size(); i < max; i++) {
          String name = v.getName(i);

          if(!name.isEmpty()) {
            index = fillParameter(index, ps, v.getValue(i));
          }
        }
      }
      case Extractor<?> r -> {}
      case Identifier i -> {}
      case Record data -> {
        for(RecordComponent recordComponent : data.getClass().getRecordComponents()) {
          try {
            index = fillParameter(index, ps, recordComponent.getAccessor().invoke(data));
          }
          catch(IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException(ex);
          }
        }
      }
      case Enum<?> e -> ps.setString(index++, e.name());
      case Instant i -> ps.setObject(index++, Timestamp.from(i));
      case LocalDate ld -> ps.setObject(index++, Date.valueOf(ld));
      case Object o -> ps.setObject(index++, o);
      case null -> ps.setNull(index++, Types.NULL);
//      default -> throw new UnsupportedOperationException("unknown type for value: " + value + " at index " + index + "; type: " + value.getClass());
    }

    return index;
  }

}