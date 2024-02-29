package org.int4.db.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

abstract class BaseTransaction<X extends Exception> implements AutoCloseable {
  private static final Logger LOGGER = System.getLogger(BaseTransaction.class.getName());
  private static final ThreadLocal<BaseTransaction<?>> CURRENT_TRANSACTION = new ThreadLocal<>();

  private static long uniqueIdentifier;

  interface ExceptionTranslator<X extends Exception> {
    X translate(BaseTransaction<X> tx, String message, SQLException e);
  }

  final Connection connection;
  final ExceptionTranslator<X> exceptionTranslator;

  private final BaseTransaction<?> parent;
  private final Savepoint savepoint;
  private final long id;
  private final boolean readOnly;
  private final List<Consumer<TransactionResult>> completionHooks = new ArrayList<>();

  private int activeNestedTransactions;
  private boolean finished;

  BaseTransaction(Supplier<Connection> connectionProvider, boolean readOnly, ExceptionTranslator<X> exceptionTranslator) throws X {
    this.parent = CURRENT_TRANSACTION.get();
    this.exceptionTranslator = exceptionTranslator;
    this.readOnly = readOnly;
    this.id = ++uniqueIdentifier;

    try {
      if(parent == null) {
        this.connection = connectionProvider.get();
        this.savepoint = null;

        connection.setAutoCommit(false);

        if(readOnly) {
          try(PreparedStatement ps = connection.prepareStatement("SET TRANSACTION READ ONLY")) {
            ps.execute();
          }
        }
      }
      else {
        this.connection = parent.connection;
        this.savepoint = connection.setSavepoint();

        parent.incrementNestedTransactions();
      }

      LOGGER.log(Level.TRACE, "New Transaction " + this);
    }
    catch(SQLException e) {
      throw exceptionTranslator.translate(this, "Exception while creating new transaction", e);
    }

    assert (this.parent != null && this.savepoint != null) || (this.parent == null && this.savepoint == null);

    CURRENT_TRANSACTION.set(this);
  }

  private void incrementNestedTransactions() {
    activeNestedTransactions++;
  }

  private void decrementNestedTransactions() {
    activeNestedTransactions--;
  }

  void ensureNotFinished() throws X {
    if(finished) {
      throw new IllegalStateException(this + ": Transaction already ended");
    }
    if(activeNestedTransactions != 0) {
      throw new IllegalStateException(this + ": Using parent transaction while nested transactions are running is not supported");
    }
  }


  // TODO what about batch inserts?
  // TODO multiple results support


  /**
   * Adds a completion hook which is called when the outer most transaction
   * completes. The passed {@link TransactionResult} is never {@code null} and
   * indicates whether the outer most transaction was committed or rolled back.
   *
   * @param consumer a consumer that is called after the outer most transaction completes, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public void addCompletionHook(Consumer<TransactionResult> consumer) {
    Objects.requireNonNull(consumer, "consumer");

    if(parent == null) {
      completionHooks.add(consumer);
    }
    else {
      parent.addCompletionHook(consumer);
    }
  }

  @SuppressWarnings("resource")
  private static void endTransaction() {
    BaseTransaction<?> parent = CURRENT_TRANSACTION.get().parent;

    if(parent == null) {
      CURRENT_TRANSACTION.remove();
    }
    else {
      CURRENT_TRANSACTION.set(parent);
    }
  }

  private void finishTransaction(boolean commit) throws X {
    ensureNotFinished();

    endTransaction();

    LOGGER.log(Level.TRACE, this + (commit ? ": COMMIT" : ": ROLLBACK"));

    try {
      if(parent == null) {
        boolean committed = false;

        try {
          if(commit) {
            connection.commit();

            committed = true;
          }
          else {
            connection.rollback();
          }
        }
        catch(SQLException e) {
          throw exceptionTranslator.translate(this, "Exception while committing/rolling back connection", e);
        }
        finally {
          for(Consumer<TransactionResult> consumer : completionHooks) {
            try {
              consumer.accept(committed ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK);
            }
            catch(Exception e) {
              LOGGER.log(Level.WARNING, "Commit hook for " + this + " threw exception: " + consumer, e);
            }
          }

          completionHooks.clear();

          try {
            connection.close();
          }
          catch(SQLException e) {
            LOGGER.log(Level.DEBUG, this + ": exception while closing connection: " + e);
          }
        }
      }
      else {
        try {
          if(commit) {
            connection.releaseSavepoint(savepoint);
          }
          else {
            connection.rollback(savepoint);
          }
        }
        catch(SQLException e) {
          throw exceptionTranslator.translate(this, "Exception while finishing nested transaction", e);
        }
        finally {
          parent.decrementNestedTransactions();
        }
      }
    }
    finally {
      finished = true;
    }
  }

  /**
   * Commits this transaction immediately. Any further attempts to use this transaction
   * will result in an exception.
   *
   * @throws X when an error occurred during the commit
   */
  public void commit() throws X {
    finishTransaction(true);
  }

  /**
   * Rolls this transaction back immediately. Any further attempts to use this transaction
   * will result in an exception.
   *
   * @throws X when an error occurred during the roll back
   */
  public void rollback() throws X {
    finishTransaction(false);
  }

  @Override
  public void close() throws X {
    if(!finished) {
      if(readOnly) {
        commit();
      }
      else {
        rollback();
      }
    }
  }

  @Override
  public String toString() {
    return String.format("T%04d%s", id, parent == null ? "" : " (" + parent + ")");
  }
}