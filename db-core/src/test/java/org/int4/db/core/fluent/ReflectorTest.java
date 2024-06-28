package org.int4.db.core.fluent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.int4.db.core.fluent.FieldValueSetParameter.Entries;
import org.int4.db.core.fluent.FieldValueSetParameter.Values;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
public class ReflectorTest {
  private static final Lookup LOOKUP = MethodHandles.lookup();
  private static final Reflector<Flat> FLAT = Reflector.of(Flat.class);
  private static final Reflector<Employee> INLINED = Reflector.of(Employee.class)
    .inline("company", Reflector.of(Company.class).inline("location", Reflector.of(Coordinate.class)))
    .inline("trip", Reflector.of(LOOKUP, Trip.class).inline("start", Reflector.of(Coordinate.class)).inline("end", Reflector.of(Coordinate.class)));

  @Test
  void shouldCreateReflectorForPublicType() {
    Reflector<Coordinate> reflector = Reflector.of(Coordinate.class);

    assertThat(reflector).isNotNull();
    assertThat(reflector.names()).isEqualTo(List.of("x", "y"));
  }

  @Test
  void shouldCreateReflectorForPrivateType() {
    Reflector<Trip> reflector = Reflector.of(LOOKUP, Trip.class);

    assertThat(reflector).isNotNull();
    assertThat(reflector.names()).isEqualTo(List.of("start", "end"));
  }

  @Test
  void customReflectorCreationShouldRejectIllegalArguments() {
    Function<Row, Coordinate> creator = row -> { return null; };
    BiFunction<Coordinate, Integer, Object> dataExtractor = (t, i) -> { return null; };

    assertThatThrownBy(() -> Reflector.custom(null, List.of("x", "y"), List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, null, List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x", "y"), null, creator, dataExtractor))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x", "y"), List.of(int.class, int.class), null, dataExtractor))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x", "y"), List.of(int.class, int.class), creator, null))
      .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, Arrays.asList("x", null), List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x", "y"), Arrays.asList(int.class, null), creator, dataExtractor))
      .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x"), List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("must specify an equal number of names and types");

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of(), List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("must specify at least one field name");

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x", "x"), List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("duplicate identifiers found in field names: [x, x]");

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, List.of("x", "1"), List.of(int.class, int.class), creator, dataExtractor))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("invalid identifier found in field names: 1");
  }

  @Test
  void shouldRejectInliningNonExistingField() {
    Reflector<Trip> reflector = Reflector.of(LOOKUP, Trip.class);

    assertThatThrownBy(() -> reflector.inline("middle", Reflector.of(Coordinate.class)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("can't inline field with name 'middle'; must be one of: [start, end]");
  }

  @Test
  void shouldRejectInliningIllegalField() {
    Reflector<Trip> reflector = Reflector.of(LOOKUP, Trip.class);

    assertThatThrownBy(() -> reflector.inline("start", Reflector.of(Coordinate.class)).inline("start_x", Reflector.of(Coordinate.class)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("can't inline field with name 'start_x'; must be one of: [end]");
  }

  @Test
  void shouldRejectInliningMismatchingType() {
    Reflector<Trip> reflector = Reflector.of(LOOKUP, Trip.class);

    assertThatThrownBy(() -> reflector.inline("start", Reflector.of(Company.class)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("reflector for inlining field 'start' must be of type: class org.int4.db.core.fluent.ReflectorTest$Coordinate");
  }

  @Test
  void shouldRejectCreatingReflectorsWithMismatchingFieldNameCount() {
    assertThatThrownBy(() -> FLAT.withNames("a"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("fieldNames must have length 8");
  }

  @Test
  void shouldCreateReflectorsWithNewNames() {
    assertThat(FLAT.withNames("a", "b", "c", "d", "e", "f", "g", "h").names())
      .isEqualTo(List.of("a", "b", "c", "d", "e", "f", "g", "h"));
  }

  @Test
  void shouldPrefixFields() {
    record SubRecord(String a, String b, int x) {}
    record DuplicateFieldTestRecord(String a, int x, SubRecord b) {}

    Reflector<SubRecord> subRecordReflector = Reflector.of(LOOKUP, SubRecord.class);
    Reflector<DuplicateFieldTestRecord> reflector = Reflector.of(LOOKUP, DuplicateFieldTestRecord.class)
      .inline("b", subRecordReflector);

    assertThat(reflector.names()).isEqualTo(List.of("a", "x", "b_a", "b_b", "b_x"));
  }

  enum ReflectorCase {
    FLAT_CASE(
      FLAT,
      new Flat("John", "Acme", 3, 4, 1, 2, 5, 6)
    ),

    INLINED_CASE(
      INLINED,
      new Employee("John", new Company("Acme", new Coordinate(3, 4)), new Trip(new Coordinate(1, 2), new Coordinate(5, 6)))
    ),

    INLINED_CASE_RENAMED(
      Reflector.of(Employee2.class).withNames("name", "company", "trip")
        .inline("company", Reflector.of(Company.class).inline("location", Reflector.of(Coordinate.class)))
        .inline("trip", Reflector.of(LOOKUP, Trip.class).inline("start", Reflector.of(Coordinate.class)).inline("end", Reflector.of(Coordinate.class))),
      new Employee2("John", new Company("Acme", new Coordinate(3, 4)), new Trip(new Coordinate(1, 2), new Coordinate(5, 6)))
    ),

    BEAN(
      Reflector.ofClass(FlatBean.class),
      new FlatBean("John", "Acme", 3, 4, 1, 2, 5, 6)
    ),

    CUSTOM(
      Reflector.custom(
        Flat.class,
        List.of("name", "company_name", "company_location_x", "company_location_y", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y"),
        List.of(String.class, String.class, int.class, int.class, int.class, int.class, int.class, int.class),
        row -> new Flat(row.getString(0), row.getString(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7)),
        (t, index) -> switch(index) {
          case 0 -> t.name();
          case 1 -> t.companyName();
          case 2 -> t.companyLocationX();
          case 3 -> t.companyLocationY();
          case 4 -> t.tripStartX();
          case 5 -> t.tripStartY();
          case 6 -> t.tripEndX();
          case 7 -> t.tripEndY();
          default -> throw new IllegalArgumentException();
        }
      ),
      new Flat("John", "Acme", 3, 4, 1, 2, 5, 6)
    ),

    PARTIAL_FLAT(
      Reflector.of(PartialFlat.class)
        .inline("company_location", Reflector.of(Coordinate.class)),
      new PartialFlat("John", "Acme", new Coordinate(3, 4), 1, 2, 5, 6)
    ),

    CUSTOM_INLINE(
      Reflector.custom(
        PartialFlat.class,
        List.of("name", "company_name", "company_location", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y"),
        List.of(String.class, String.class, Coordinate.class, int.class, int.class, int.class, int.class),
        row -> new PartialFlat(row.getString(0), row.getString(1), row.getObject(2, Coordinate.class), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6)),
        (t, index) -> switch(index) {
          case 0 -> t.name();
          case 1 -> t.companyName();
          case 2 -> t.companyLocation();
          case 3 -> t.tripStartX();
          case 4 -> t.tripStartY();
          case 5 -> t.tripEndX();
          case 6 -> t.tripEndY();
          default -> throw new IllegalArgumentException();
        }
      )
      .inline("company_location", Reflector.of(Coordinate.class)),
      new PartialFlat("John", "Acme", new Coordinate(3, 4), 1, 2, 5, 6)
    );

    final Reflector<Object> reflector;
    final Object testObject;

    @SuppressWarnings("unchecked")
    <T> ReflectorCase(Reflector<T> reflector, T testObject) {
      this.reflector = (Reflector<Object>)reflector;
      this.testObject = testObject;
    }
  }

  @Nested
  class WhenReflectorCreated {

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldExcludeFields(ReflectorCase c) {
      assertThat(c.reflector.excluding("company_location_x", "company_location_y").names())
        .isEqualTo(List.of("name", "company_name", "", "", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y"));
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldRejectExcludingNonExistingFields(ReflectorCase c) {
      assertThatThrownBy(() -> c.reflector.excluding("company_location_x", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("unable to exclude non-existing fields: [b]");
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldKeepFields(ReflectorCase c) {
      assertThat(c.reflector.only("company_location_x", "company_location_y").names())
        .isEqualTo(List.of("", "", "company_location_x", "company_location_y", "", "", "", ""));
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

      assertThat(baseValues.names()).isEqualTo(List.of("name", "company_name", "company_location_x", "company_location_y", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y"));
      assertThat(baseValues.getValue(0)).isEqualTo("John");
      assertThat(baseValues.getValue(1)).isEqualTo("Acme");
      assertThat(baseValues.getValue(2)).isEqualTo(3);
      assertThat(baseValues.getValue(3)).isEqualTo(4);
      assertThat(baseValues.getValue(4)).isEqualTo(1);
      assertThat(baseValues.getValue(5)).isEqualTo(2);
      assertThat(baseValues.getValue(6)).isEqualTo(5);
      assertThat(baseValues.getValue(7)).isEqualTo(6);
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldCreateEntries(ReflectorCase c) {
      Entries baseValues = c.reflector.entries(c.testObject);

      assertThat(baseValues.names()).isEqualTo(List.of("name", "company_name", "company_location_x", "company_location_y", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y"));
      assertThat(baseValues.getValue(0)).isEqualTo("John");
      assertThat(baseValues.getValue(1)).isEqualTo("Acme");
      assertThat(baseValues.getValue(2)).isEqualTo(3);
      assertThat(baseValues.getValue(3)).isEqualTo(4);
      assertThat(baseValues.getValue(4)).isEqualTo(1);
      assertThat(baseValues.getValue(5)).isEqualTo(2);
      assertThat(baseValues.getValue(6)).isEqualTo(5);
      assertThat(baseValues.getValue(7)).isEqualTo(6);
    }

    @Nested
    class AndARowNeedsMapping {
      private final Row row = Row.of("John", "Acme", 3, 4, 1, 2, 5, 6);

      @ParameterizedTest
      @EnumSource(ReflectorCase.class)
      void shouldCreateRecord(ReflectorCase c) {
        assertThat(c.reflector.apply(row)).isEqualTo(c.testObject);
      }
    }
  }

  public static class FlatBean {
    private String name;
    private String companyName;
    private int companyLocationX;
    private int companyLocationY;
    private int tripStartX;
    private int tripStartY;
    private int tripEndX;
    private int tripEndY;

    public FlatBean(String name, String companyName, int companyLocationX, int companyLocationY, int tripStartX, int tripStartY, int tripEndX, int tripEndY) {
      this.name = name;
      this.companyName = companyName;
      this.companyLocationX = companyLocationX;
      this.companyLocationY = companyLocationY;
      this.tripStartX = tripStartX;
      this.tripStartY = tripStartY;
      this.tripEndX = tripEndX;
      this.tripEndY = tripEndY;
    }

    public String getName() {
      return name;
    }

    public String getCompanyName() {
      return companyName;
    }

    public int getCompanyLocationX() {
      return companyLocationX;
    }

    public int getCompanyLocationY() {
      return companyLocationY;
    }

    public int getTripStartX() {
      return tripStartX;
    }

    public int getTripStartY() {
      return tripStartY;
    }

    public int getTripEndX() {
      return tripEndX;
    }

    public int getTripEndY() {
      return tripEndY;
    }

    @Override
    public int hashCode() {
      return Objects.hash(companyLocationX, companyLocationY, companyName, name, tripEndX, tripEndY, tripStartX, tripStartY);
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if(obj == null) {
        return false;
      }
      if(getClass() != obj.getClass()) {
        return false;
      }
      FlatBean other = (FlatBean)obj;
      return companyLocationX == other.companyLocationX && companyLocationY == other.companyLocationY && Objects.equals(companyName, other.companyName) && Objects.equals(name, other.name) && tripEndX == other.tripEndX && tripEndY == other.tripEndY && tripStartX == other.tripStartX && tripStartY == other.tripStartY;
    }
  }

  public record Flat(String name, String companyName, int companyLocationX, int companyLocationY, int tripStartX, int tripStartY, int tripEndX, int tripEndY) {}
  public record PartialFlat(String name, String companyName, Coordinate companyLocation, int tripStartX, int tripStartY, int tripEndX, int tripEndY) {}
  public record Employee(String name, Company company, Trip trip) {}
  public record Employee2(String naam, Company bedrijf, Trip trip) {}
  private record Trip(Coordinate start, Coordinate end) {}
  public record Company(String name, Coordinate location) {}
  public record Coordinate(int x, int y) {}
}
