package org.int4.db.core.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.int4.db.core.fluent.SQLResult;
import org.int4.db.core.reflect.Extractor;
import org.int4.db.core.reflect.Identifier;
import org.int4.db.core.reflect.Row;
import org.int4.db.core.reflect.RowAccessException;
import org.int4.db.core.reflect.TypeConverter;
import org.int4.db.core.reflect.FieldValueSetParameter.Entries;
import org.int4.db.core.reflect.FieldValueSetParameter.Values;
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
  private final Map<Class<?>, TypeConverter<?, ?>> typeConverters;

  /**
   * Constructs a new instance.
   *
   * @param template a {@link StringTemplate}, cannot be {@code null}
   * @param typeConverters a map of {@link TypeConverter}s, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public SafeSQL(StringTemplate template, Map<Class<?>, TypeConverter<?, ?>> typeConverters) {
    this.values = template.values();
    this.typeConverters = Map.copyOf(Objects.requireNonNull(typeConverters, "typeConverters"));
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
      final boolean isBatch;

      {
        ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        isBatch = fillParameters(ps, values);
      }

      @Override
      public SQLResult execute() throws SQLException {
        if(isBatch) {
          ps.executeBatch();
        }
        else {
          ps.execute();
        }

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
        final DynamicRow row = new DynamicRow(typeConverters, rs);

        boolean nextResult;
        boolean nextCalled;

        @Override
        public boolean hasNext() {
          if(!nextCalled) {
            try {
              nextResult = rs.next();
              nextCalled = true;
            }
            catch(SQLException e) {
              throw new RowAccessException(e);
            }
          }

          return nextResult;
        }

        @Override
        public Row next() {
          if(!hasNext()) {
            throw new NoSuchElementException();
          }

          nextCalled = false;

          return row;
        }
      };
    }
    catch(SQLException e) {
      throw new RowAccessException(e);
    }
  }

  private String createSQL(StringTemplate template) {
    StringBuilder sb = new StringBuilder();
    List<String> fragments = template.fragments();
    List<Object> values = template.values();

    for(int i = 0; i < values.size(); i++) {
      String fragment = fragments.get(i);
      Object value = values.get(i);

      sb.append(fragment);

      if(value instanceof List<?> l) {
        if(l.isEmpty()) {
          throw new IllegalArgumentException("parameter " + i + " of type " + l.getClass() + " should not be empty: " + template);
        }

        // Use first element of any list to build up the prepared statement:
        value = l.getFirst();
      }

      appendTemplateValue(sb, value, fragment);
    }

    sb.append(fragments.getLast());

    return sb.toString();
  }

  private void appendTemplateValue(StringBuilder sb, Object value, String fragment) {
    switch(value) {
      case Extractor<?> r -> {
        Matcher matcher = ALIAS.matcher(fragment);
        String alias = matcher.matches() ? matcher.group(2) + "." : "";

        if(!alias.isEmpty()) {
          sb.delete(sb.length() - matcher.group(1).length(), sb.length());
        }

        sb.append(r.names().stream().filter(NOT_EMPTY).map(n -> alias + n).collect(Collectors.joining(", ")));
      }
      case Entries e -> sb.append(e.names().stream().filter(NOT_EMPTY).map(t -> t + " = ?").collect(Collectors.joining(", ")));
      case Values v -> sb.append(v.names().stream().filter(NOT_EMPTY).map(t -> "?").collect(Collectors.joining(", ")));
      case Identifier i -> sb.append(i.getIdentifier());
      default -> {
        TypeConverter<?, ?> converter = typeConverters.get(value.getClass());

        switch(value) {
          case Record r when converter == null -> {
            RecordComponent[] recordComponents = r.getClass().getRecordComponents();

            for (int j = 0; j < recordComponents.length; j++) {
              if (j != 0) {
                sb.append(", ");
              }

              sb.append("?");
            }
          }
          default -> sb.append("?");
        }
      }
    }
  }

  private boolean fillParameters(PreparedStatement ps, List<Object> values) throws SQLException {
    int batchSize = -1;

    for(Object value : values) {
      int size = value instanceof List<?> l ? l.size() : value instanceof Values v ? v.batchSize() : 1;

      assert size != 0 : "batches cannot be empty";  // shouldn't be able to get here with size == 0

      if(size > 1) {  // size of 1 is not considered a batch
        if(batchSize == -1) {
          batchSize = size;
        }
        else if(size != batchSize) {
          throw new IllegalArgumentException("batches are of different sizes");
        }
      }
    }

    if(batchSize == -1) {
      batchSize = 1;
    }

    for(int row = 0; row < batchSize; row++) {
      int index = 1;

      for(Object value : values) {
        if(value instanceof List<?> l) {
          index = fillParameter(row, index, ps, l.get(row));
        }
        else {
          index = fillParameter(row, index, ps, value);
        }
      }

      if(batchSize > 1) {
        ps.addBatch();
      }
    }

    return batchSize > 1;
  }

  private int fillParameter(int row, int startIndex, PreparedStatement ps, Object value) throws SQLException {
    int index = startIndex;

    switch(value) {
      case null -> ps.setNull(index++, Types.NULL);
      case Entries e -> {
        for(int i = 0, max = e.size(); i < max; i++) {
          String name = e.getName(i);

          if(!name.isEmpty()) {
            index = fillParameter(row, index, ps, e.getValue(row, i));
          }
        }
      }
      case Values v -> {
        for(int i = 0, max = v.size(); i < max; i++) {
          String name = v.getName(i);

          if(!name.isEmpty()) {
            index = fillParameter(row, index, ps, v.getValue(row, i));
          }
        }
      }
      case Extractor<?> r -> {}
      case Identifier i -> {}
      default -> {
        @SuppressWarnings("unchecked")
        TypeConverter<Object, Object> converter = (TypeConverter<Object, Object>) typeConverters.get(value.getClass());

        switch(value) {
          case Object o when converter != null -> ps.setObject(index++, converter.encode(value));
          case Enum<?> e -> ps.setString(index++, e.name());
          case Record data -> {
            for(RecordComponent recordComponent : data.getClass().getRecordComponents()) {
              try {
                index = fillParameter(row, index, ps, recordComponent.getAccessor().invoke(data));
              }
              catch(IllegalAccessException | InvocationTargetException ex) {
                throw new IllegalStateException(ex);
              }
            }
          }
          default -> {
            ps.setObject(index++, value);
          }
        }
      }
    }

    return index;
  }

}