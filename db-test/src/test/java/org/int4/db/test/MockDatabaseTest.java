package org.int4.db.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.int4.db.core.api.DatabaseException;
import org.int4.db.core.fluent.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class MockDatabaseTest {
  private MockDatabase db;

  @BeforeEach
  void beforeEach() {
    this.db = new MockDatabase();

    db.mockQuery("SELECT \\* FROM employees", List.of(
      Row.of(1, "Jane"),
      Row.of(2, "John")
    ));

    db.mockQuery("SELECT aap .*", () -> { throw new DatabaseException("Unknown column 'aap'", new SQLException()); });

    db.mockExecute("INSERT INTO nonexisting .*", () -> { throw new DatabaseException("table does not exist", new SQLException()); });
    db.mockExecute("INSERT INTO other .*", () -> { throw new DatabaseException("other table does not exist", new SQLException()); });

    db.mockUpdate("UPDATE employees .*", 1);
  }

  @Test
  void queryToListShouldReturnRows() {
    List<Row> rows = db.query(tx -> tx."SELECT * FROM employees".toList());

    assertThat(rows).isEqualTo(List.of(
      Row.of(1, "Jane"),
      Row.of(2, "John")
    ));

    rows = db.query(tx -> tx."SELECT * FROM empty".toList());

    assertThat(rows).isEmpty();
  }

  @Test
  void queryAsIntToListShouldReturnRows() {
    List<Integer> rows = db.query(tx -> tx."SELECT * FROM employees".asInt().toList());

    assertThat(rows).isEqualTo(List.of(1, 2));
  }

  @Test
  void queryConsumeShouldReturnRows() {
    List<Row> rows = new ArrayList<>();

    try(var tx = db.beginReadOnlyTransaction()) {
      tx."SELECT * FROM employees".consume(rows::add);
    }

    assertThat(rows).isEqualTo(List.of(
      Row.of(1, "Jane"),
      Row.of(2, "John")
    ));

    try(var tx = db.beginReadOnlyTransaction()) {
      assertThat(tx."SELECT * FROM employees".consume(rows::add, 2)).isFalse();
      assertThat(tx."SELECT * FROM employees".consume(rows::add, 1)).isTrue();
      assertThat(tx."SELECT * FROM employees".consume(rows::add, 0)).isTrue();
    }
  }

  @Test
  void queryShouldThrowException() {
    assertThatThrownBy(() -> db.query(tx -> tx."SELECT aap FROM employees".toList()))
      .isInstanceOf(DatabaseException.class)
      .hasMessage("Unknown column 'aap'");
  }

  @Test
  void insertShouldThrowException() {
    assertThatThrownBy(() -> db.accept(tx -> tx."INSERT INTO nonexisting (a, b, c) VALUES (?, ?, ?)".execute()))
      .isInstanceOf(DatabaseException.class)
      .hasMessage("table does not exist");

    assertThatThrownBy(() -> db.accept(tx -> tx."INSERT INTO other (a, b, c) VALUES (?, ?, ?)".execute()))
      .isInstanceOf(DatabaseException.class)
      .hasMessage("other table does not exist");

    assertDoesNotThrow(() -> db.accept(tx -> tx."INSERT INTO employees".execute()));
  }

  @Test
  void updateShouldReturnCount() {
    long count = db.apply(tx -> tx."UPDATE employees SET name = ?".executeUpdate());

    assertThat(count).isEqualTo(1);

    count = db.apply(tx -> tx."UPDATE donothing SET name = ?".executeUpdate());

    assertThat(count).isEqualTo(0);
  }
}
