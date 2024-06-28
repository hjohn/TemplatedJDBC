package org.int4.db.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import javax.sql.DataSource;

import org.int4.db.core.api.Database;
import org.int4.db.core.api.TypeConverter;
import org.int4.db.core.fluent.Reflector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

import io.zonky.test.db.postgres.junit5.EmbeddedPostgresExtension;
import io.zonky.test.db.postgres.junit5.PreparedDbExtension;

public class TypeConvertersIT {
  @RegisterExtension
  private static final PreparedDbExtension POSTGRES = EmbeddedPostgresExtension.preparedDatabase(ds -> {});
  private static final Lookup LOOKUP = MethodHandles.lookup();

  private Database database;

  @BeforeEach
  void beforeEach() throws SQLException {
    DataSource dataSource = POSTGRES.getDbProvider().createDataSource();

    database = DatabaseBuilder
      .using(() -> {
        try {
          return dataSource.getConnection();
        }
        catch(SQLException e) {
          throw new IllegalStateException(e);
        }
      })
      .addTypeConverter(Custom.class, TypeConverter.of(String.class, Custom::value, Custom::new))
      .addTypeConverter(Instant.class, TypeConverter.of(Timestamp.class, Timestamp::from, Timestamp::toInstant))
      .addTypeConverter(State.class, TypeConverter.of(Integer.class, State::ordinal, i -> State.values()[i]))
      .build();
  }

  private static final Reflector<Company> ALL = Reflector.of(LOOKUP, Company.class);

  private enum Type {NON_PROFIT, CORPORATE}
  private enum State {STARTUP, ESTABLISHED}
  private record Company(int id, String name, Custom custom, Instant creationTime, Type type, State state) {}
  static record Custom(String value) {}

  @Test
  void shouldDoTypeConversions() {
    database.accept(tx ->
      tx."CREATE TABLE company (id int4, name varchar(100), custom varchar, creation_time timestamptz, type varchar, state int4)".execute()
    );

    Company input = new Company(1, "Acme", new Custom("Bla"), Instant.ofEpochMilli(1), Type.CORPORATE, State.ESTABLISHED);

    database.accept(tx -> tx."INSERT INTO company (\{ALL}) VALUES (\{ALL.values(input)})".execute());

    Company output = database.query(tx -> tx."SELECT \{ALL} FROM company WHERE id = 1".map(ALL).get());

    assertThat(output).isEqualTo(input);
  }

  @Test
  void shouldHandleNulls() {
    database.accept(tx ->
      tx."CREATE TABLE company (id int4, name varchar(100), custom varchar, creation_time timestamptz, type varchar, state int4)".execute()
    );

    Company input = new Company(1, null, null, null, null, null);

    database.accept(tx -> tx."INSERT INTO company (\{ALL}) VALUES (\{ALL.values(input)})".execute());

    Company output = database.query(tx -> tx."SELECT \{ALL} FROM company WHERE id = 1".map(ALL).get());

    assertThat(output).isEqualTo(input);
  }
}
