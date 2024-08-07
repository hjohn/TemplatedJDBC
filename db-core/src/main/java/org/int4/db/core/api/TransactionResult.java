package org.int4.db.core.api;

/**
 * The final status of a transaction.
 */
public enum TransactionResult {

  /**
   * The transaction was committed.
   */
  COMMITTED,

  /**
   * The transaction was rolled back.
   */
  ROLLED_BACK;

}