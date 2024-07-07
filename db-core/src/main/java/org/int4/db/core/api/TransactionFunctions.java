package org.int4.db.core.api;

import java.lang.StringTemplate.Processor;
import java.util.function.Consumer;

import org.int4.db.core.fluent.StatementNode;

interface TransactionFunctions<X extends Exception> extends AutoCloseable, Processor<StatementNode<X>, X> {

  /**
   * Commits this transaction immediately. Any further attempts to use this transaction
   * will result in an exception.
   *
   * @throws X when an error occurred during the commit
   */
  void commit() throws X;

  /**
   * Rolls this transaction back immediately. Any further attempts to use this transaction
   * will result in an exception.
   *
   * @throws X when an error occurred during the roll back
   */
  void rollback() throws X;

  /**
   * Adds a completion hook which is called when the outer most transaction
   * completes. The passed {@link TransactionResult} is never {@code null} and
   * indicates whether the outer most transaction was committed or rolled back.
   *
   * @param consumer a consumer that is called after the outer most transaction completes, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  void addCompletionHook(Consumer<TransactionResult> consumer);

  @Override
  void close() throws X;
}
