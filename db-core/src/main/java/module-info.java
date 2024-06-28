module org.int4.db.core {
  requires transitive java.sql;

  exports org.int4.db.core.api;
  exports org.int4.db.core.fluent;
  exports org.int4.db.core.util;
  exports org.int4.db.core.internal to org.int4.db.test;
}