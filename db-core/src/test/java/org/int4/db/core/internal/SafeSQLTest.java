package org.int4.db.core.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.int4.db.core.api.TypeConverter;
import org.int4.db.core.fluent.Extractor;
import org.int4.db.core.fluent.Identifier;
import org.int4.db.core.fluent.Reflector;
import org.int4.db.core.fluent.Row;
import org.int4.db.core.fluent.SQLResult;
import org.int4.db.core.util.MockResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.lang.StringTemplate.RAW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SafeSQLTest {
  private static final Lookup LOOKUP = MethodHandles.lookup();
  private static final Map<Class<?>, TypeConverter<?, ?>> TYPE_CONVERTERS = Map.of(LocalDate.class, TypeConverter.of(Date.class, Date::valueOf, Date::toLocalDate));

  @Captor
  private ArgumentCaptor<String> sqlCaptor;

  @SuppressWarnings("resource")
  @Test
  void shouldCreatePreparedStatement(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);
    Extractor<Employee> nameOnly = all.only("name");
    Employee employee = new Employee("John", null, LocalDate.of(1234, 5, 6), 42.42, true, Gender.M);
    boolean overtime = false;

    StringTemplate template = RAW."""
      INSERT INTO employees (\{nameOnly}) VALUES (\{nameOnly.values(employee)});
      INSERT INTO employees (\{all}) VALUES (\{employee});
      SELECT * FROM \{Identifier.of("employees")} WHERE overtime = \{overtime} AND \{nameOnly.entries(employee)}
    """;

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("""
      INSERT INTO employees (name) VALUES (?);
      INSERT INTO employees (name, middle_name, birth_date, salary, overtime, gender) VALUES (?, ?, ?, ?, ?, ?);
      SELECT * FROM employees WHERE overtime = ? AND name = ?
    """);

    verify(preparedStatement).setObject(1, "John");
    verify(preparedStatement).setObject(2, "John");
    verify(preparedStatement).setNull(3, Types.NULL);
    verify(preparedStatement).setObject(4, Date.valueOf(LocalDate.of(1234, 5, 6)));
    verify(preparedStatement).setObject(5, 42.42);
    verify(preparedStatement).setObject(6, true);
    verify(preparedStatement).setString(7, "M");
    verify(preparedStatement).setObject(8, false);
    verify(preparedStatement).setObject(9, "John");

    assertThat(statement.toString()).isEqualTo("""
      INSERT INTO employees (name) VALUES (?);
      INSERT INTO employees (name, middle_name, birth_date, salary, overtime, gender) VALUES (?, ?, ?, ?, ?, ?);
      SELECT * FROM employees WHERE overtime = ? AND name = ?
    """);
  }

  @SuppressWarnings("resource")
  @Test
  void shouldCreatePreparedStatementWithAlias(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);
    SafeSQL sql = new SafeSQL(RAW."SELECT e.\{all} FROM employee e", TYPE_CONVERTERS);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("SELECT e.name, e.middle_name, e.birth_date, e.salary, e.overtime, e.gender FROM employee e");

    verifyNoMoreInteractions(preparedStatement);

    assertThat(statement.toString()).isEqualTo("SELECT e.name, e.middle_name, e.birth_date, e.salary, e.overtime, e.gender FROM employee e");
  }

  @SuppressWarnings("resource")
  @Test
  void shouldCreateAndExecuteBatchStatement(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);
    List<Employee> employees = new ArrayList<>();

    for(int i = 0; i < 10; i++) {
      employees.add(new Employee("John" + i, null, LocalDate.of(1234, 5, 6 + i), 42.42, true, Gender.M));
    }

    StringTemplate template = RAW."""
      INSERT INTO employees (\{all}) VALUES (\{all.batch(employees)})
    """;

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("""
      INSERT INTO employees (name, middle_name, birth_date, salary, overtime, gender) VALUES (?, ?, ?, ?, ?, ?)
    """);

    SQLResult result = statement.execute();

    verify(preparedStatement).executeBatch();

    when(preparedStatement.getGeneratedKeys()).thenReturn(new MockResultSet(List.of(
      List.of(1001),
      List.of(1002),
      List.of(1003)
    )));

    Iterator<Row> iterator = result.createGeneratedKeysIterator();

    assertThat(iterator.next()).extracting(Row::toArray).isEqualTo(new Object[] {1001});
    assertThat(iterator.next()).extracting(Row::toArray).isEqualTo(new Object[] {1002});
    assertThat(iterator.next()).extracting(Row::toArray).isEqualTo(new Object[] {1003});
    assertThat(iterator.hasNext()).isFalse();
  }

  @SuppressWarnings("resource")
  @Test
  void shouldExecuteBatchStatementAsSingleWhenOnlyOneEntry(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);
    List<Employee> employees = new ArrayList<>();

    employees.add(new Employee("John", null, LocalDate.of(1234, 5, 6), 42.42, true, Gender.M));

    StringTemplate template = RAW."""
      INSERT INTO employees (\{all}) VALUES (\{all.batch(employees)})
    """;

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("""
      INSERT INTO employees (name, middle_name, birth_date, salary, overtime, gender) VALUES (?, ?, ?, ?, ?, ?)
    """);

    SQLResult result = statement.execute();

    verify(preparedStatement).execute();

    when(preparedStatement.getGeneratedKeys()).thenReturn(new MockResultSet(List.of(
      List.of(1001)
    )));

    Iterator<Row> iterator = result.createGeneratedKeysIterator();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).extracting(Row::toArray).isEqualTo(new Object[] {1001});
    assertThat(iterator.hasNext()).isFalse();
  }

  @SuppressWarnings("resource")
  @Test
  void shouldRejectExecutingBatchesOfDifferentSizes(@Mock Connection connection) {
    Extractor<Employee> left = Reflector.of(LOOKUP, Employee.class).only("name", "middle_name", "birth_date");
    Extractor<Employee> right = Reflector.of(LOOKUP, Employee.class).only("salary", "overtime", "gender");
    List<Employee> leftEmployees = new ArrayList<>();
    List<Employee> rightEmployees = new ArrayList<>();

    for(int i = 0; i < 5; i++) {
      leftEmployees.add(new Employee("John" + i, null, LocalDate.of(1234, 5, 6 + i), 42.42, true, Gender.M));
      rightEmployees.add(new Employee("John" + i, null, LocalDate.of(1234, 5, 6 + i), 42.42, true, Gender.M));
    }

    for(int i = 5; i < 10; i++) {
      leftEmployees.add(new Employee("John" + i, null, LocalDate.of(1234, 5, 6 + i), 42.42, true, Gender.M));
    }

    StringTemplate template = RAW."""
      INSERT INTO employees (\{left}, \{right}) VALUES (\{left.batch(leftEmployees)}, \{right.batch(rightEmployees)})
    """;

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    assertThatThrownBy(() -> sql.toSQLStatement(connection))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("batches are of different sizes");
  }

  @SuppressWarnings("resource")
  @Test
  void shouldSupportListsForBatches(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    List<String> strings = List.of("a", "b", "c");
    List<Integer> numbers = List.of(1, 2, 3);

    StringTemplate template = RAW."""
      INSERT INTO alphabet (letter, index) VALUES (\{strings}, \{numbers})
    """;

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("""
      INSERT INTO alphabet (letter, index) VALUES (?, ?)
    """);

    statement.execute();

    verify(preparedStatement).executeBatch();

    InOrder inOrder = Mockito.inOrder(preparedStatement);

    inOrder.verify(preparedStatement).setObject(1, "a");
    inOrder.verify(preparedStatement).setObject(2, 1);
    inOrder.verify(preparedStatement).addBatch();
    inOrder.verify(preparedStatement).setObject(1, "b");
    inOrder.verify(preparedStatement).setObject(2, 2);
    inOrder.verify(preparedStatement).addBatch();
    inOrder.verify(preparedStatement).setObject(1, "c");
    inOrder.verify(preparedStatement).setObject(2, 3);
    inOrder.verify(preparedStatement).addBatch();

    verifyNoMoreInteractions(preparedStatement);
  }

  @SuppressWarnings("resource")
  @Test
  void shouldSupportListsAndConstantsForBatches(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    List<String> strings = List.of("a", "b", "c");
    int number = 1;

    StringTemplate template = RAW."INSERT INTO alphabet (letter, index) VALUES (\{strings}, \{number})";

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("INSERT INTO alphabet (letter, index) VALUES (?, ?)");

    statement.execute();

    verify(preparedStatement).executeBatch();

    InOrder inOrder = Mockito.inOrder(preparedStatement);

    inOrder.verify(preparedStatement).setObject(1, "a");
    inOrder.verify(preparedStatement).setObject(2, 1);
    inOrder.verify(preparedStatement).addBatch();
    inOrder.verify(preparedStatement).setObject(1, "b");
    inOrder.verify(preparedStatement).setObject(2, 1);
    inOrder.verify(preparedStatement).addBatch();
    inOrder.verify(preparedStatement).setObject(1, "c");
    inOrder.verify(preparedStatement).setObject(2, 1);
    inOrder.verify(preparedStatement).addBatch();

    verifyNoMoreInteractions(preparedStatement);
  }

  @Test
  void shouldRejectEmptyListAsTemplateParameter() {
    List<String> strings = List.of();
    int number = 1;

    StringTemplate template = RAW."INSERT INTO alphabet (letter, index) VALUES (\{strings}, \{number})";

    assertThatThrownBy(() -> new SafeSQL(template, TYPE_CONVERTERS))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("parameter 0 of type class java.util.ImmutableCollections$ListN should not be empty: StringTemplate{ fragments = [ \"INSERT INTO alphabet (letter, index) VALUES (\", \", \", \")\" ], values = [[], 1] }");
  }

  @SuppressWarnings("resource")
  @Test
  void shouldCreateAndExecuteSelectStatement(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    Reflector<Employee> all = Reflector.of(LOOKUP, Employee.class);

    StringTemplate template = RAW."SELECT \{all} FROM employees";

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    assertThat(sql.getSQL()).isEqualTo("SELECT name, middle_name, birth_date, salary, overtime, gender FROM employees");
    assertThat(sql.toString()).isEqualTo("SELECT name, middle_name, birth_date, salary, overtime, gender FROM employees");

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("SELECT name, middle_name, birth_date, salary, overtime, gender FROM employees");

    SQLResult result = statement.execute();

    when(preparedStatement.getResultSet()).thenReturn(new MockResultSet(List.of(
      Arrays.asList("John", null, LocalDate.of(1234, 5, 6), 42.42, true, Gender.M)
    )));

    Iterator<Row> iterator = result.createIterator();

    assertThat(iterator.next()).extracting(Row::toArray).isEqualTo(new Object[] {"John", null, LocalDate.of(1234, 5, 6), 42.42, true, Gender.M});
    assertThat(iterator.hasNext()).isFalse();
  }

  @SuppressWarnings("resource")
  @Test
  void shouldCreateAndExecuteUpdateStatement(@Mock Connection connection, @Mock PreparedStatement preparedStatement) throws SQLException {
    StringTemplate template = RAW."UPDATE employees SET salary = 100";

    SafeSQL sql = new SafeSQL(template, TYPE_CONVERTERS);

    assertThat(sql.getSQL()).isEqualTo("UPDATE employees SET salary = 100");
    assertThat(sql.toString()).isEqualTo("UPDATE employees SET salary = 100");

    when(connection.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatement);

    SQLStatement statement = sql.toSQLStatement(connection);

    assertThat(sqlCaptor.getValue()).isEqualTo("UPDATE employees SET salary = 100");

    SQLResult result = statement.execute();

    when(preparedStatement.getLargeUpdateCount()).thenReturn(42L);

    assertThat(result.getUpdateCount()).isEqualTo(42);
  }

  enum Gender {M, F}
  record Employee(String name, String middleName, LocalDate birthDate, double salary, boolean overtime, Gender gender) {}
}
