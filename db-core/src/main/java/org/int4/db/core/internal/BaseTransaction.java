package org.int4.db.core.internal;

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

import org.int4.db.core.api.TransactionResult;

public abstract class BaseTransaction<X extends Exception> implements AutoCloseable {
  private static final Logger LOGGER = System.getLogger(BaseTransaction.class.getName());
  private static final ThreadLocal<BaseTransaction<?>> CURRENT_TRANSACTION = new ThreadLocal<>();

  private static long uniqueIdentifier;

  protected interface ExceptionTranslator<X extends Exception> {
    X translate(BaseTransaction<X> tx, String message, SQLException e);
  }

  private final BaseTransaction<?> parent;
  private final long id;
  private final boolean readOnly;
  private final List<Consumer<TransactionResult>> completionHooks = new ArrayList<>();
  private final Supplier<Connection> connectionSupplier;
  private final ExceptionTranslator<X> exceptionTranslator;

  private Connection connection;
  private Savepoint savepoint;
  private int activeNestedTransactions;
  private boolean finished;

  protected BaseTransaction(Supplier<Connection> connectionSupplier, boolean readOnly, ExceptionTranslator<X> exceptionTranslator) {
    this.parent = CURRENT_TRANSACTION.get();
    this.connectionSupplier = connectionSupplier;
    this.exceptionTranslator = exceptionTranslator;
    this.readOnly = readOnly;
    this.id = ++uniqueIdentifier;

    CURRENT_TRANSACTION.set(this);

    if(parent != null) {
      parent.activeNestedTransactions++;
    }

    LOGGER.log(Level.TRACE, "New Transaction " + this);
  }

  public final Connection getConnection() throws X {
    ensureNotFinished();

    return getConnectionInternal();
  }

  private Connection getConnectionInternal() throws X {
    if(connection == null) {
      try {
        if(parent == null) {
          this.connection = connectionSupplier.get();

          connection.setAutoCommit(false);

          if(readOnly) {
            try(PreparedStatement ps = connection.prepareStatement("SET TRANSACTION READ ONLY")) {
              ps.execute();
            }
          }
        }
        else {
          this.connection = parent.getConnectionInternal();
          this.savepoint = connection.setSavepoint();
        }
      }
      catch(Exception e) {
        if(e instanceof SQLException se) {
          throw exceptionTranslator.translate(this, "Exception while creating new transaction", se);
        }

        /*
         * This probably can't happen, but needed because the generic exception type of parent isn't fully known.
         * Either it throws a DatabaseException which is runtime, and so it won't get here, or it is an SQLException
         * in which case the if above handles it.
         */

        throw new IllegalStateException("Unexpected type of exception", e);
      }
    }

    return connection;
  }

  private void ensureNotFinished() {
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

  private void finishTransaction(boolean commit) throws X {
    ensureNotFinished();

    LOGGER.log(Level.TRACE, this + (commit ? ": COMMIT" : ": ROLLBACK"));

    finished = true;

    if(parent != null) {
      endNestedTransaction(commit);
    }
    else {
      boolean noException = false;

      try {
        endTopLevelTransaction(commit);
        noException = true;
      }
      finally {
        TransactionResult result = noException && commit ? TransactionResult.COMMITTED : TransactionResult.ROLLED_BACK;

        for(Consumer<TransactionResult> consumer : completionHooks) {
          try {
            consumer.accept(result);
          }
          catch(Exception e) {
            LOGGER.log(Level.WARNING, "Commit hook for " + this + " threw exception: " + consumer, e);
          }
        }

        completionHooks.clear();
      }
    }
  }

  private void endNestedTransaction(boolean commit) throws X {
    parent.activeNestedTransactions--;

    CURRENT_TRANSACTION.set(parent);

    if(connection != null) {
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
    }
  }

  private void endTopLevelTransaction(boolean commit) throws X {
    CURRENT_TRANSACTION.remove();

    if(connection != null) {
      try {
        if(commit) {
          connection.commit();
        }
        else {
          connection.rollback();
        }
      }
      catch(SQLException e) {
        throw exceptionTranslator.translate(this, "Exception while committing/rolling back connection", e);
      }
      finally {
        try {
          connection.close();
        }
        catch(SQLException e) {
          LOGGER.log(Level.DEBUG, this + ": exception while closing connection: " + e);
        }
      }
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