package org.int4.db.core.fluent;

interface StatementSteps<X extends Exception> {

  /**
   * Switches a statement which does not normally return rows (like {@code INSERT}s) to
   * a source returning rows (often these are generated keys). The columns of the
   * returned rows are undefined, and depend on the JDBC driver used. Often databases
   * allow to specify what such statements should return (for example, PostgreSQL supports
   * the {@code RETURNING} clause on {@code INSERT} statements).
   *
   * <p>The behavior of this call is undefined when called on statements that do
   * not have an alternative row source.
   *
   * @return a row source node, never {@code null}
   */
  RowSourceNode<X> mapGeneratedKeys();

  /**
   * Executes the statement as a statement that returns a number of affected rows.
   * Calling this on a statement that returns rows, or nothing will result in an
   * exception.
   *
   * @return the number of affected rows, never negative
   * @throws X when execution fails
   */
  long executeUpdate() throws X;

  /**
   * Executes the statement without expecting a result.
   *
   * @throws X when execution fails
   */
  void execute() throws X;

}
