package org.int4.db.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.int4.db.core.api.Database;
import org.int4.db.core.api.DatabaseException;
import org.int4.db.core.api.Transaction;
import org.int4.db.core.reflect.Extractor;
import org.int4.db.core.reflect.Reflector;
import org.int4.db.core.reflect.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.zonky.test.db.postgres.junit5.EmbeddedPostgresExtension;
import io.zonky.test.db.postgres.junit5.PreparedDbExtension;

public class DatabaseIT {
  @RegisterExtension
  private static final PreparedDbExtension POSTGRES = EmbeddedPostgresExtension.preparedDatabase(ds -> {});
  private static final Lookup LOOKUP = MethodHandles.lookup();

  private Database database;

  @BeforeEach
  void beforeEach() throws SQLException {
    DataSource dataSource = POSTGRES.getDbProvider().createDataSource();

    database = DatabaseBuilder.using(() -> {
      try {
        return dataSource.getConnection();
      }
      catch(SQLException e) {
        throw new IllegalStateException(e);
      }
    }).build();
  }

  @Nested
  class WhenCompanyTableCreated {
    private record Company(int id, String name, Instant foundingTime, double genderRatio, boolean royal) {}

    private static final Reflector<Company> ALL = Reflector.of(LOOKUP, Company.class);

    @BeforeEach
    void beforeEach() {
      database.accept(tx ->
        tx."CREATE TABLE company (id int4, name varchar(100), founding_time timestamptz, gender_ratio float8, royal bool)".execute()
      );
    }

    @Test
    void selectShouldReturnNoRecords() {
      database.query(tx ->
        assertThat(tx."SELECT COUNT(*) FROM company".asInt().get()).isEqualTo(0)
      );
    }

    @Test
    void shouldDoBatchInsert() {
      List<Company> companies = new ArrayList<>();

      for(int i = 1; i <= 100; i++) {
        companies.add(new Company(i, "Company #" + i, Instant.ofEpochSecond(i), 0.5, false));
      }

      database.accept(tx ->
        tx."INSERT INTO company (\{ALL}) VALUES (\{ALL.batch(companies)})".execute()
      );

      List<Company> results = database.query(tx -> tx."SELECT \{ALL} FROM company ORDER BY id".map(ALL).toList());

      assertThat(results).containsExactlyElementsOf(companies);
    }

    @Nested
    class AndCompaniesWereAdded {
      private final Company company1 = new Company(1, "Acme", Instant.ofEpochSecond(0), 0.5, false);
      private final Company company2 = new Company(2, "Unlimited Ltd", Instant.parse("2007-12-03T10:15:30.00Z"), 0.9, true);

      @BeforeEach
      void beforeEach() {
        database.accept(tx -> {
          assertThat(tx."INSERT INTO company (\{ALL}) VALUES (\{ALL.values(company1)})".executeUpdate()).isEqualTo(1);
          assertThat(tx."INSERT INTO company (\{ALL}) VALUES (\{ALL.values(company2)})".executeUpdate()).isEqualTo(1);
        });
      }

      @Test
      void selectShouldReturnRecords() {
        List<Company> list = database.query(tx ->
          tx."SELECT \{ALL} FROM company ORDER BY name DESC".map(ALL).toList()
        );

        assertThat(list).containsExactly(company2, company1);
      }

      @Test
      void selectShouldReturnRawRecords() {
        List<Row> list = database.query(tx ->
          tx."SELECT \{ALL} FROM company ORDER BY name DESC".toList()
        );

        assertThat(list).containsExactly(
          Row.of(company2.id, company2.name, Timestamp.from(company2.foundingTime), company2.genderRatio, company2.royal),
          Row.of(company1.id, company1.name, Timestamp.from(company1.foundingTime), company1.genderRatio, company1.royal)
        );
      }
    }
  }

  @Nested
  class WhenEmployeeTableCreated {
    private static final Reflector<Employee> ALL = Reflector.of(LOOKUP, Employee.class);
    private static final Reflector<Composite> COMPOSITE = Reflector.of(LOOKUP, Composite.class)
      .inline("other_data", Reflector.of(LOOKUP, OtherData.class))
      .withNames("id", "name", "age", "data");
    private static final Extractor<Employee> EXCEPT_ID = ALL.excluding("id");

    @BeforeEach
    void beforeEach() {
      try(Transaction tx = database.beginTransaction()) {
        tx."CREATE TABLE employee (id serial4, name varchar(100), age int4, data bytea)".execute();
        tx.commit();
      }
    }

    @Test
    void selectShouldReturnNoRecords() {
      try(Transaction tx = database.beginTransaction()) {
        assertThat(tx."SELECT COUNT(*) FROM employee".asInt().get()).isEqualTo(0);
      }
    }

    @Nested
    class AndEmployeesWereAdded {
      private final Employee employeeJane = new Employee(null, "Jane Doe", 43, new byte[] {1, 2, 3});
      private final Employee employeeJohn = new Employee(null, "John Doe", 32, new byte[] {3, 4, 5});

      @BeforeEach
      void beforeEach() {
        try(Transaction tx = database.beginTransaction()) {
          Employee insertedEmployee = tx."INSERT INTO employee (\{EXCEPT_ID}) VALUES (\{EXCEPT_ID.values(employeeJohn)})"
            .mapGeneratedKeys()
            .asInt()
            .map(employeeJohn::withId)
            .get();

          assertThat(insertedEmployee).isEqualTo(employeeJohn.withId(1));

          assertThat(tx."INSERT INTO employee (\{EXCEPT_ID}) VALUES (\{EXCEPT_ID.values(employeeJane)})".executeUpdate()).isEqualTo(1);

          tx.commit();
        }
      }

      @Test
      void selectShouldReturnRecords() {
        try(Transaction tx = database.beginReadOnlyTransaction()) {
          List<Employee> list = tx."SELECT \{ALL} FROM employee ORDER BY name".map(ALL).toList();

          assertThat(list).containsExactly(
            employeeJane.withId(2),
            employeeJohn.withId(1)
          );
        }

        try(Transaction tx = database.beginReadOnlyTransaction()) {
          List<Composite> list = tx."SELECT \{COMPOSITE} FROM employee ORDER BY name DESC".map(COMPOSITE).toList();

          assertThat(list).containsExactly(
            new Composite(1, "John Doe", new OtherData(32, new byte[] {3, 4, 5})),
            new Composite(2, "Jane Doe", new OtherData(43, new byte[] {1, 2, 3}))
          );
        }

        try(Transaction tx = database.beginReadOnlyTransaction()) {
          List<NameOnly> list = tx."SELECT name FROM employee ORDER BY name".asString().map(NameOnly::new).toList();

          assertThat(list).containsExactly(
            new NameOnly("Jane Doe"),
            new NameOnly("John Doe")
          );
        }
      }

      @Nested
      class AndAnEmployeeWasUpdated {
        private final Employee employeeAlice = new Employee(null, "Alice Brooks", 51, new byte[] {5, 6, 7});

        @BeforeEach
        void beforeEach() {
          try(Transaction tx = database.beginTransaction()) {
            assertThat(tx."UPDATE employee SET \{EXCEPT_ID.entries(employeeAlice)} WHERE name = \{"John Doe"}".executeUpdate()).isEqualTo(1);
            tx.commit();
          }
        }

        @Test
        void selectShouldReturnUpdatedEmployeeRecords() {
          try(Transaction tx = database.beginReadOnlyTransaction()) {
            List<Employee> list = tx."SELECT \{ALL} FROM employee ORDER BY name".map(ALL).toList();

            assertThat(list).containsExactly(employeeAlice.withId(1), employeeJane.withId(2));
          }
        }
      }
    }
  }

  private record NameOnly(String name) {}
  private record Employee(Integer id, String name, int age, byte[] data) {
    public Employee withId(int id) {
      return new Employee(id, name, age, data);
    }

    @Override
    public int hashCode() {
      int result = 1;

      result = 31 * result + Arrays.hashCode(data);
      result = 31 * result + Objects.hash(age, id, name);

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if(obj == null || getClass() != obj.getClass()) {
        return false;
      }

      Employee other = (Employee)obj;

      return age == other.age && Arrays.equals(data, other.data) && Objects.equals(id, other.id) && Objects.equals(name, other.name);
    }
  }

  private record Composite(int id, String name, OtherData otherData) {}
  private record OtherData(int age, byte[] data) {

    @Override
    public int hashCode() {
      int result = 1;

      result = 31 * result + Arrays.hashCode(data);
      result = 31 * result + Objects.hash(age);

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if(obj == null || getClass() != obj.getClass()) {
        return false;
      }

      OtherData other = (OtherData)obj;

      return age == other.age && Arrays.equals(data, other.data);
    }

  }

  @Test
  void shouldNotAllowModificationsWhenReadOnly() {
    try(Transaction tx = database.beginReadOnlyTransaction()) {
      assertThatThrownBy(() -> tx."CREATE TABLE employee (id serial4, name varchar(100), age int4, data bytea)".execute())
        .isInstanceOf(DatabaseException.class)
        .hasRootCauseMessage("ERROR: cannot execute CREATE TABLE in a read-only transaction");
    }
  }
}
