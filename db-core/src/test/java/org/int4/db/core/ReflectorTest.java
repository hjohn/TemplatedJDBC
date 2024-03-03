package org.int4.db.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.List;

import org.int4.db.core.fluent.FieldValueSetParameter.Entries;
import org.int4.db.core.fluent.FieldValueSetParameter.Values;
import org.int4.db.core.fluent.Reflector;
import org.int4.db.core.fluent.Row;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReflectorTest {
  private static final Lookup LOOKUP = MethodHandles.lookup();

  @Test
  void shouldCreateReflectors() {
    assertThat(Reflector.of(LOOKUP, Angle.class)).isNotNull();
  }

  @Test
  void shouldCreateReflectorForPublicType() {
    assertThat(Reflector.of(Base.class)).isNotNull();
  }

  @Test
  void shouldRejectCreatingReflectorsWithMismatchingFieldNameCount() {
    assertThatThrownBy(() -> Reflector.of(LOOKUP, Base.class).withNames("a"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("fieldNames must have length 5");
  }

  @Test
  void shouldCreateReflectorsWithNewNames() {
    assertThat(Reflector.of(LOOKUP, Base.class).withNames("a", "b", "c", "d", "e").names())
      .isEqualTo(List.of("a", "b", "c", "d", "e"));
  }

  @Test
  void shouldRenameDuplicateFields() {
    record SubRecord(String a, String a2, int x) {}
    record DuplicateFieldTestRecord(String a, int x, SubRecord a2) {}

    assertThat(Reflector.of(LOOKUP, DuplicateFieldTestRecord.class).names()).isEqualTo(List.of("a", "x", "a_3", "a_2", "x_2"));
  }

  enum ReflectorCase {
    SIMPLE(
      Reflector.of(LOOKUP, Base.class),
      new Base("John", 3, 4, 5, 45.0)
    ),

    COMPOUND(
      Reflector.of(LOOKUP, Compound.class),
      new Compound("John", new Coordinate(new Coordinate2D(3, 4), 5), new Angle(45.0))
    ),

    CUSTOM(
      Reflector.custom(
        List.of("a", "x", "y", "z", "angle"),
        row -> new Base(row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getDouble(4)),
        (t, index) -> switch(index) {
          case 0 -> t.a();
          case 1 -> t.x();
          case 2 -> t.y();
          case 3 -> t.z();
          case 4 -> t.angle();
          default -> throw new IllegalArgumentException();
        }
      ),
      new Base("John", 3, 4, 5, 45.0)
    );

    final Reflector<Object> reflector;
    final Object testObject;

    @SuppressWarnings("unchecked")
    ReflectorCase(Reflector<?> reflector, Object testObject) {
      this.reflector = (Reflector<Object>)reflector;
      this.testObject = testObject;
    }
  }

  @Nested
  class WhenReflectorCreated {

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldExcludeFields(ReflectorCase c) {
      assertThat(c.reflector.excluding("x", "y", "z").names()).isEqualTo(List.of("a", "", "", "", "angle"));
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldRejectExcludingNonExistingFields(ReflectorCase c) {
      assertThatThrownBy(() -> c.reflector.excluding("x", "y", "z", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("unable to exclude non-existing fields: [b]");
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldKeepFields(ReflectorCase c) {
      assertThat(c.reflector.only("x", "y").names()).isEqualTo(List.of("", "x", "y", "", ""));
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldRejectKeepingNonExistingFields(ReflectorCase c) {
      assertThatThrownBy(() -> c.reflector.only("b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("unable to keep non-existing fields: [b]");
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldCreateValues(ReflectorCase c) {
      Values baseValues = c.reflector.values(c.testObject);

      assertThat(baseValues.names()).isEqualTo(List.of("a", "x", "y", "z", "angle"));
      assertThat(baseValues.getValue(0)).isEqualTo("John");
      assertThat(baseValues.getValue(1)).isEqualTo(3);
      assertThat(baseValues.getValue(2)).isEqualTo(4);
      assertThat(baseValues.getValue(3)).isEqualTo(5);
      assertThat(baseValues.getValue(4)).isEqualTo(45.0);
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldCreateEntries(ReflectorCase c) {
      Entries baseValues = c.reflector.entries(c.testObject);

      assertThat(baseValues.names()).isEqualTo(List.of("a", "x", "y", "z", "angle"));
      assertThat(baseValues.getValue(0)).isEqualTo("John");
      assertThat(baseValues.getValue(1)).isEqualTo(3);
      assertThat(baseValues.getValue(2)).isEqualTo(4);
      assertThat(baseValues.getValue(3)).isEqualTo(5);
      assertThat(baseValues.getValue(4)).isEqualTo(45.0);
    }

    @Nested
    class AndARowNeedsMapping {
      private final Row row;

      AndARowNeedsMapping(@Mock Row row) {
        this.row = row;

        when(row.getString(0)).thenReturn("John");
        when(row.getInt(1)).thenReturn(3);
        when(row.getInt(2)).thenReturn(4);
        when(row.getInt(3)).thenReturn(5);
        when(row.getDouble(4)).thenReturn(45.0);
      }

      @ParameterizedTest
      @EnumSource(ReflectorCase.class)
      void shouldCreateRecord(ReflectorCase c) {
        assertThat(c.reflector.apply(row)).isEqualTo(c.testObject);
      }
    }

//    @Nested
//    class AndExtractorIsDerivedWithGap {
//      private final Extractor<Base> holedReflector = reflector.excluding("z");
//
//      @Test
//      void shouldCreateMapperWithMatchingHole() {
//        assertThat(holedReflector.asMapperFor(Base2D.class)).isNotNull();
//      }
//
//      @Nested
//      class AndARowNeedsMapping {
//        private final Row rs;
//
//        AndARowNeedsMapping(@Mock Row rs) throws SQLException {
//          this.rs = rs;
//
//          when(rs.getString(1)).thenReturn("Alice");
//          when(rs.getInt(2)).thenReturn(5);
//          when(rs.getInt(3)).thenReturn(6);
//          when(rs.getDouble(4)).thenReturn(45.0);
//        }
//
//        @Test
//        void shouldCreateBase2DRecord() throws SQLException {
//          assertThat(holedReflector.asMapperFor(Base2D.class).apply(rs)).isEqualTo(new Base2D("Alice", 5, 6, 45.0));
//        }
//      }
//    }
  }

  public record Base(String a, int x, int y, int z, double angle) {}

  private record Compound(String a, Coordinate c, Angle angle) {}
  private record Coordinate(Coordinate2D coord2d, int z) {}
  private record Coordinate2D(int x, int y) {}
  private record Angle(double angle) {}
}
