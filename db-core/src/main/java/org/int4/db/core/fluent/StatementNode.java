package org.int4.db.core.fluent;

import org.int4.db.core.internal.bridge.Context;
import org.int4.db.core.internal.bridge.SQLResult;

/**
 * Fluent node for an SQL statement.
 *
 * @param <X> the exception type that can be thrown
 */
public class StatementNode<X extends Exception> extends RowSourceNode<X> implements StatementSteps<X> {
  private final Context<X> context;

  /**
   * Constructs a new instance.
   *
   * @param context a context, cannot be {@code null}
   * @throws NullPointerException when any argument is {@code null}
   */
  public StatementNode(Context<X> context) {
    super(context, SQLResult::createIterator);

    this.context = context;
  }

  @Override
  public RowSourceNode<X> mapGeneratedKeys() {
    return new RowSourceNode<>(context, SQLResult::createGeneratedKeysIterator);
  }

  @Override
  public long executeUpdate() throws X {
    return context.executeUpdate();
  }

  @Override
  public void execute() throws X {
    context.execute();
  }
}