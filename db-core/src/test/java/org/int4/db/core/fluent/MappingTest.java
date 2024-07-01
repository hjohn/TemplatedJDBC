package org.int4.db.core.fluent;

import org.int4.db.core.util.ThrowingFunction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MappingTest {
  private static final ThrowingFunction<Company, Location, Throwable> EXTRACTOR = Company::location;

  @Test
  void shouldRejectInvalidArguments() {
    assertThatThrownBy(() -> Mapping.of(null, Location.class, EXTRACTOR)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Mapping.of("x", null, EXTRACTOR)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Mapping.of("x", Location.class, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldCreateFieldMappingWithValidParameters() {
    Mapping.Field<Company, Location> mapping = Mapping.of("validName", Location.class, EXTRACTOR);

    assertThat(mapping).isNotNull();
    assertThat(mapping.name()).isEqualTo("validName");
    assertThat(mapping.type()).isEqualTo(Location.class);
    assertThat(mapping.extractor()).isEqualTo(EXTRACTOR);
  }

  @Test
  void shouldCreateMappingWithReflector() {
    Reflector<Location> reflector = Reflector.of(Location.class);
    Mapping.Inline<Company, Location> mapping = Mapping.inline(EXTRACTOR, reflector);

    assertThat(mapping).isNotNull();
    assertThat(mapping.extractor()).isEqualTo(EXTRACTOR);
    assertThat(mapping.reflector()).isEqualTo(reflector);
  }

  public record Company(String name, Location location) {}
  public record Location(int x, int y) {}
}
