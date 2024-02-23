package org.int4.db.core.fluent;

import java.util.regex.Pattern;

public final class Identifier {
  private static final Pattern VALID_IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  public static Identifier of(String identifier) {
    return new Identifier(identifier);
  }

  static boolean isValidIdentifier(String text) {
    return VALID_IDENTIFIER.matcher(text).matches();
  }

  private final String identifier;

  private Identifier(String identifier) {
    if(!VALID_IDENTIFIER.matcher(identifier).matches()) {
      throw new IllegalArgumentException("identifier must be a valid identifier: " + identifier);
    }

    this.identifier = identifier;
  }

  public String getIdentifier() {
    return identifier;
  }
}
