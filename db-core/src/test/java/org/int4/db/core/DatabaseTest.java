package org.int4.db.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.int4.db.core.api.Database;
import org.int4.db.core.api.Transaction;
import org.int4.db.core.api.TransactionResult;
import org.int4.db.core.internal.BaseTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import nl.altindag.log.LogCaptor;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DatabaseTest {
  private Supplier<Connection> connectionProvider;
  private Database database;

  @Mock private Connection connection;
  @Mock private Savepoint savepoint;
  @Mock private PreparedStatement statement;

  @BeforeEach
  public void before() throws SQLException {
    when(connection.setSavepoint()).thenReturn(savepoint);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(connection.prepareStatement(anyString(), anyInt())).thenReturn(statement);

    connectionProvider = new Supplier<>() {
      @Override
      public Connection get() {
        return connection;
      }
    };

    database = DatabaseBuilder.using(connectionProvider).build();
  }

  @Test
  public void shouldAutoRollbackTransaction() throws SQLException {
    assertThrows(IllegalArgumentException.class, () -> {
      try(Transaction transaction = database.beginTransaction()) {
        transaction."".execute();  // simulate statement being executed

        throw new IllegalArgumentException();
      }
    });

    verify(connection).rollback();
    verify(connection, never()).commit();
  }

  @Test
  public void shouldCommitTransaction() throws SQLException {
    try(Transaction transaction = database.beginTransaction()) {
      transaction."".execute();  // simulate statement being executed
      transaction.commit();
    }

    verify(connection).commit();
    verify(connection, never()).rollback();
  }

  @Test
  public void shouldNotCommitOrRollbackUnusedTransaction() {
    try(Transaction transaction = database.beginTransaction()) {
      transaction.commit();
    }

    verifyNoInteractions(connection);
  }

  @Test
  public void shouldNotAllowCommitAfterRollback() {
    try(Transaction transaction = database.beginTransaction()) {
      transaction.rollback();

      assertThrows(IllegalStateException.class, () -> transaction.commit());
    }
  }

  @Test
  public void shouldNotAllowRollbackAfterCommit() {
    try(Transaction transaction = database.beginTransaction()) {
      transaction.commit();

      assertThrows(IllegalStateException.class, () -> transaction.rollback());
    }
  }

  @Test
  public void shouldAllowNestedTransaction() throws SQLException {
    try(Transaction transaction = database.beginTransaction()) {
      try(Transaction nestedTransaction = database.beginTransaction()) {
        nestedTransaction."".execute();  // simulate statement being executed
        nestedTransaction.commit();
      }

      verify(connection).releaseSavepoint(any(Savepoint.class));
      verify(connection, never()).rollback();
      verify(connection, never()).commit();

      transaction."".execute();  // simulate statement being executed
      transaction.commit();
    }

    verify(connection).commit();
  }

  @Test
  public void shouldRollbackNestedTransaction() throws SQLException {
    try(Transaction transaction = database.beginTransaction()) {
      try(Transaction nestedTransaction = database.beginTransaction()) {
        nestedTransaction."".execute();  // simulate statement being executed
        nestedTransaction.rollback();
      }

      verify(connection).rollback(any(Savepoint.class));
      verify(connection, never()).rollback();
      verify(connection, never()).commit();

      transaction."".execute();  // simulate statement being executed
      transaction.commit();
    }

    verify(connection).commit();
  }

  @Test
  public void shouldNotAllowUncommitedNestedTransactions() {
    try(Transaction transaction = database.beginTransaction()) {
      try(Transaction nestedTransaction = database.beginTransaction()) {
        assertThrows(IllegalStateException.class, () -> transaction.commit());
      }
    }
  }

  @Test
  public void shouldCommitReadOnlyTransactions() throws SQLException {
    try(Transaction transaction = database.beginReadOnlyTransaction()) {
      transaction."".execute();  // simulate statement being executed
    }

    verify(connection).commit();
  }

  @Test
  public void shouldNotCommitOrRollbackUnusedReadOnlyTransactions() {
    try(Transaction transaction = database.beginReadOnlyTransaction()) {
    }

    verifyNoInteractions(connection);
  }

  @Test
  public void shouldCallCompletionHookOnceOuterTransactionCompletes() {
    AtomicReference<TransactionResult> reference = new AtomicReference<>();
    AtomicReference<TransactionResult> nestedReference = new AtomicReference<>();

    try(Transaction transaction = database.beginTransaction()) {
      transaction.addCompletionHook(reference::set);

      try(Transaction nestedTransaction = database.beginTransaction()) {
        nestedTransaction.addCompletionHook(nestedReference::set);
        nestedTransaction.commit();
      }

      assertThat(reference.get()).isNull();
      assertThat(nestedReference.get()).isNull();
    }

    assertThat(reference.get()).isEqualTo(TransactionResult.ROLLED_BACK);
    assertThat(nestedReference.get()).isEqualTo(TransactionResult.ROLLED_BACK);
  }

  @Test
  public void shouldCallCompletionHookEvenIfOuterTransactionExitsExceptionally() throws SQLException {
    AtomicReference<TransactionResult> reference = new AtomicReference<>();

    doThrow(RuntimeException.class).when(connection).rollback();

    try(Transaction transaction = database.beginTransaction()) {
      transaction.addCompletionHook(reference::set);
      transaction."".execute();  // simulate statement being executed
    }
    catch(Exception e) {
    }

    assertThat(reference.get()).isEqualTo(TransactionResult.ROLLED_BACK);
  }

  @Test
  public void shouldIgnoreExceptionsThrownFromCompletionHook() throws SQLException {
    LogCaptor captor = LogCaptor.forClass(BaseTransaction.class);

    try(Transaction transaction = database.beginTransaction()) {
      captor.disableConsoleOutput();
      transaction.addCompletionHook(state -> {
        throw new IllegalStateException();
      });

      transaction."".execute();  // simulate statement being executed
      transaction.commit();
    }
    finally {
      captor.enableConsoleOutput();
    }

    assertThat(captor.getWarnLogs()).size().isEqualTo(1);

    verify(connection).commit();
  }
}
