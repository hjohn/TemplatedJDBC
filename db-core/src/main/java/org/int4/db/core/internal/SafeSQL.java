package org.int4.db.core.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.int4.db.core.fluent.Extractor;
import org.int4.db.core.fluent.FieldValueSetParameter.Entries;
import org.int4.db.core.fluent.FieldValueSetParameter.Values;
import org.int4.db.core.fluent.Identifier;
import org.int4.db.core.fluent.Row;
import org.int4.db.core.fluent.RowAccessException;
import org.int4.db.core.fluent.SQLResult;
import org.int4.db.core.util.ThrowingSupplier;

/**
 * Wrapper around a {@link StringTemplate} that can provide SQL strings and
 * created {@link PreparedStatement}s.
 */
public class SafeSQL {
  private static final Predicate<String> NOT_EMPTY = Predicate.not(String::isEmpty);
  private static final Pattern ALIAS = Pattern.compile(".*? (([a-zA-Z][a-zA-Z_0-9]*) *\\. *)");

  private final String sql;
  private final List<Object> values;

  /**
   * Constructs a new instance.
   *
   * @param template a {@link StringTemplate}, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public SafeSQL(StringTemplate template) {
    this.values = template.values();
    this.sql = createSQL(template);
  }

  /**
   * Returns the generated SQL string.
   *
   * @return the generated SQL string, never {@code null}
   */
  public String getSQL() {
    return sql;
  }

  @Override
  public String toString() {
    return sql;
  }

  public SQLStatement toSQLStatement(Connection connection) throws SQLException {
    return new SQLStatement() {
      final PreparedStatement ps;

      {
        ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        fillParameters(ps, values);
      }

      @Override
      public SQLResult execute() throws SQLException {
        ps.execute();

        return new SQLResult() {
          @Override
          public Iterator<Row> createIterator() {
            return createRowIterator(ps::getResultSet);
          }

          @Override
          public Iterator<Row> createGeneratedKeysIterator() {
            return createRowIterator(ps::getGeneratedKeys);
          }

          @Override
          public long getUpdateCount() {
            try {
              return ps.getLargeUpdateCount();
            }
            catch(SQLException e) {
              throw new RowAccessException(e);
            }
          }
        };
      }

      @Override
      public void close() throws SQLException {
        ps.close();
      }

      @Override
      public String toString() {
        return sql;
      }
    };
  }

  private Iterator<Row> createRowIterator(ThrowingSupplier<ResultSet, SQLException> resultSetSupplier) {
    try {
      return new Iterator<>() {
        final ResultSet rs = resultSetSupplier.get();
        final DynamicRow row = new DynamicRow(rs);

        @Override
        public boolean hasNext() {
          try {
            return rs.next();
          }
          catch(SQLException e) {
            throw new RowAccessException(e);
          }
        }

        @Override
        public Row next() {
          return row;
        }
      };
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  private static String createSQL(StringTemplate template) {
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

    return sb.toString();
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