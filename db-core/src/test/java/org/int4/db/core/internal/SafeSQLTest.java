package org.int4.db.core.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

import org.int4.db.core.fluent.Extractor;
import org.int4.db.core.fluent.Identifier;
import org.int4.db.core.fluent.Reflector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.lang.StringTemplate.RAW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SafeSQLTest {
  private static final Lookup LOOKUP = MethodHandles.lookup();

  @Captor
  private ArgumentCaptor<String> sqlCaptor;

  @SuppressWarnings("resource")
  @Test
  void shouldCreatePreparedStatement(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);
    Extractor<Employee> nameOnly = all.only("name");
    Employee employee = new Employee("John", LocalDate.of(1234, 5, 6), 42.42, true, Gender.M);
    boolean overtime = false;

    StringTemplate template = RAW."""
      INSERT INTO employees (\{nameOnly}) VALUES (\{nameOnly.values(employee)});
      INSERT INTO employees (\{all}) VALUES (\{employee});
      SELECT * FROM \{Identifier.of("employees")} WHERE overtime = \{overtime} AND \{nameOnly.entries(employee)}
    """;

    SafeSQL sql = new SafeSQL(template);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("""
      INSERT INTO employees (name) VALUES (?);
      INSERT INTO employees (name, birth_date, salary, overtime, gender) VALUES (?,?,?,?,?);
      SELECT * FROM employees WHERE overtime = ? AND name = ?
    """);

    verify(preparedStatement).setObject(1, "John");
    verify(preparedStatement).setObject(2, "John");
    verify(preparedStatement).setObject(3, Date.valueOf(LocalDate.of(1234, 5, 6)));
    verify(preparedStatement).setObject(4, 42.42);
    verify(preparedStatement).setObject(5, true);
    verify(preparedStatement).setString(6, "M");
    verify(preparedStatement).setObject(7, false);
    verify(preparedStatement).setObject(8, "John");

    assertThat(sql.toString()).isEqualTo("""
      INSERT INTO employees (name) VALUES (?);
      INSERT INTO employees (name, birth_date, salary, overtime, gender) VALUES (?,?,?,?,?);
      SELECT * FROM employees WHERE overtime = ? AND name = ?
    """);
  }

  @SuppressWarnings("resource")
  @Test
  void shouldCreatePreparedStatementWithAlias(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);
    SafeSQL sql = new SafeSQL(RAW."SELECT e.\{all} FROM employee e");

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("SELECT e.name, e.birth_date, e.salary, e.overtime, e.gender FROM employee e");

    verifyNoMoreInteractions(preparedStatement);
  }

  enum Gender {M, F}
  record Employee(String name, LocalDate birthDate, double salary, boolean overtime, Gender gender) {}
}
