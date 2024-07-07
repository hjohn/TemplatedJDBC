package org.int4.db.core.reflect;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

interface NameTranslator {
  static final NameTranslator UNDERSCORED = new NameTranslator() {
    private static final Pattern PATTERN = Pattern.compile("(?<!^)(?=[A-Z](?![A-Z]|$))|(?<=[a-z])(?![a-z]|$)");

    @Override
    public String toDatabaseName(String javaName) {
      return PATTERN.splitAsStream(javaName).map(part -> part.toLowerCase()).collect(Collectors.joining("_"));
    }
  };

  String toDatabaseName(String javaName);
}
