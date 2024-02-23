package org.int4.db.core.fluent;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents an SQL identifier.
 */
public final class Identifier {
  private static final Pattern VALID_IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  /**
   * Creates a new SQL identifier.
   *
   * @param identifier a text to use to create the identifier, cannot be {@code null}
   * @return a new identifier, never {@code null}
   * @throws NullPointerException when any argument is {@code null}
   * @throws IllegalArgumentException when the given identifier is not a valid SQL identifier
   */
  public static Identifier of(String identifier) {
    return new Identifier(identifier);
  }

  static boolean isValidIdentifier(String text) {
    return VALID_IDENTIFIER.matcher(text).matches();
  }

  private final String identifier;

  private Identifier(String identifier) {
    if(!VALID_IDENTIFIER.matcher(Objects.requireNonNull(identifier, "identifier")).matches()) {
      throw new IllegalArgumentException("identifier must be a valid identifier: " + identifier);
    }

    this.identifier = identifier;
  }

  /**
   * Returns the SQL identifier as a {@link String}.
   *
   * @return the identifier, never {@code null}
   */
  public String getIdentifier() {
    return identifier;
  }
}
