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

  public static void main(String[] args) {
    String x = "thisIsATest: \"this_is_a_test\"\n"
      + "EndWithNumber3: \"end_with_number_3\"\n"
      + "3ThisStartWithNumber: \"3_this_start_with_number\"\n"
      + "Number3InMiddle: \"number_3_in_middle\"\n"
      + "Number3inMiddleAgain: \"number_3_in_middle_again\"\n"
      + "MyUUIDNot: \"my_uuid_not\"\n"
      + "HOLAMundo: \"hola_mundo\"\n"
      + "holaMUNDO: \"hola_mundo\"\n"
      + "with_underscore: \"with_underscore\"\n"
      + "withAUniqueLetter: \"with_a_unique_letter\"\n"
      + "with%SYMBOLAndNumber90: \"with_%_symbol_and_number_90\"\n"
      + "http%: \"http_%\"\n"
      + "123456789: \"123456789\"\n"
      + "getUUIDAsString: \"123456789\"\n"
//      + "     : \"     \"\n"
      + "_: \"_\"\n"
      + "__abc__: \"__abc__\"";

    for(String line : x.lines().toList()) {
      String[] parts = line.split(":");
//      System.out.println(">>> " + parts[0] + " -> " + parts[1]);

      if(!("\"" + UNDERSCORED.toDatabaseName(parts[0]) + "\"").equals(parts[1].trim())) {
        System.out.println("Fails on: " + line + " with " + UNDERSCORED.toDatabaseName(parts[0]));
      }
    }
  }
}
