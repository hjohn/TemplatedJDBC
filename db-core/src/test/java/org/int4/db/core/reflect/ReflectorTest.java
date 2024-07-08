package org.int4.db.core.reflect;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.int4.db.core.reflect.FieldValueSetParameter.Entries;
import org.int4.db.core.reflect.FieldValueSetParameter.Values;
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
  private static final TypeConverter<Car, String> CAR_CONVERTER = TypeConverter.of(String.class, car -> car.brand() + ";" + car.type(), str -> new Car(str.split(";")[0], str.split(";")[1]));
  private static final Lookup LOOKUP = MethodHandles.lookup();
  private static final Reflector<Flat> FLAT = Reflector.of(Flat.class);
  private static final Reflector<Coordinate> COORDINATE = Reflector.of(Coordinate.class);
  private static final Reflector<Company> COMPANY = Reflector.of(Company.class)
    .addTypeConverter(Car.class, CAR_CONVERTER);
  private static final Reflector<Trip> TRIP = Reflector.of(Trip.class);
  private static final Reflector<Employee> INLINED = Reflector.of(LOOKUP, Employee.class)
    .nest("company", COMPANY.nest("location", COORDINATE))
    .nest("trip", TRIP
      .nest("start", COORDINATE)
      .nest("end", COORDINATE)
    );

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
    Mapping<Coordinate, Integer> m1 = Mapping.of("x", int.class, Coordinate::x);
    Mapping<Coordinate, Integer> m2 = Mapping.of("y", int.class, Coordinate::y);

    assertThatThrownBy(() -> Reflector.custom(null, creator, List.of(m1, m2)))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("type");
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, null, List.of(m1, m2)))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("creator");
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, creator, null))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("mappings");
    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, creator, Arrays.asList(m1, null)))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("mappings[1]");

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, creator, List.of()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("must specify at least one mapping");

    assertThatThrownBy(() -> Reflector.custom(Coordinate.class, creator, List.of(m1, m1)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("names cannot contain duplicate names, but found duplicate: x in: [x, x]");
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
      .hasMessage("reflector for inlining field 'start' must be of type: class org.int4.db.core.reflect.ReflectorTest$Coordinate");
  }

  @Test
  void shouldRejectCreatingReflectorsWithMismatchingFieldNameCount() {
    assertThatThrownBy(() -> FLAT.withNames("a"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("fieldNames must have length 10");
  }

  @Test
  void shouldCreateReflectorsWithNewNames() {
    assertThat(FLAT.withNames("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").names())
      .isEqualTo(List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));

    assertThat(INLINED.withNames("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").names())
      .isEqualTo(List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
  }

  @Test
  void shouldRejectCreatingReflectorUsingIllegalNames() {
    assertThatThrownBy(() -> FLAT.withNames("a", "b", "c", "d", "e", "f", "g", "h", "i", "j-illegal"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("name must be a valid identifier: j-illegal");
  }

  @Test
  void prefixShouldPrefixNames() {
    assertThat(FLAT.prefix("f_").names())
      .isEqualTo(List.of("f_name", "f_company_name", "f_company_location_x", "f_company_location_y", "f_company_car", "f_trip_start_x", "f_trip_start_y", "f_trip_end_x", "f_trip_end_y", "f_age"));

    assertThat(INLINED.prefix("f_").names())
      .isEqualTo(List.of("f_name", "f_company_name", "f_company_location_x", "f_company_location_y", "f_company_car", "f_trip_start_x", "f_trip_start_y", "f_trip_end_x", "f_trip_end_y", "f_age"));
  }

  @Test
  void inlineShouldUseExistingFieldNames() {
    Reflector<Company> reflector = COMPANY.inline("location", COORDINATE);

    assertThat(reflector.names()).isEqualTo(List.of("name", "x", "y", "car"));
  }

  @Test
  void nestShouldPrefixFields() {
    Reflector<Company> reflector = COMPANY.nest("location", COORDINATE);

    assertThat(reflector.names()).isEqualTo(List.of("name", "location_x", "location_y", "car"));
  }

  @Test
  void accessingAnIllegalColumnShouldFail() {
    Reflector<Trip> reflector = TRIP
      .inline("start", Reflector.custom(
        Coordinate.class,
        row -> new Coordinate(row.getInt(0), row.getInt(2)),  // custom uses wrong column, which should throw exception
        List.of(Mapping.of("x", int.class, Coordinate::x), Mapping.of("y", int.class, Coordinate::y))
      ))
      .nest("end", COORDINATE);

    assertThat(reflector.names()).isEqualTo(List.of("x", "y", "end_x", "end_y"));
    assertThatThrownBy(() -> reflector.apply(Row.of(1, 2, 3, 4)))
      .isInstanceOf(IndexOutOfBoundsException.class)
      .hasMessage("Index 2 out of bounds for length 2");
  }

  enum ReflectorCase {
    FLAT_CASE(
      FLAT,
      new Flat("John", "Acme", 3, 4, "BMW;3", 1, 2, 5, 6, 49),
      new Flat(null, "Acme", 3, 4, null, 1, 2, 5, 6, 49)
    ),

    INLINED_CASE(
      INLINED,
      new Employee("John", new Company("Acme", new Coordinate(3, 4), new Car("BMW", "3")), new Trip(new Coordinate(1, 2), new Coordinate(5, 6)), 49),
      new Employee(null, new Company("Acme", new Coordinate(3, 4), null), new Trip(new Coordinate(1, 2), new Coordinate(5, 6)), 49)
    ),

    INLINED_CASE_RENAMED(
      Reflector.of(Employee2.class).withNames("name", "company", "trip", "age")
        .nest("company", Reflector.of(Company.class)
          .nest("location", Reflector.of(Coordinate.class))
          .addTypeConverter(Car.class, CAR_CONVERTER)
        )
        .nest("trip", Reflector.of(LOOKUP, Trip.class).nest("start", Reflector.of(Coordinate.class)).nest("end", Reflector.of(Coordinate.class))),
      new Employee2("John", new Company("Acme", new Coordinate(3, 4), new Car("BMW", "3")), new Trip(new Coordinate(1, 2), new Coordinate(5, 6)), 49),
      new Employee2(null, new Company("Acme", new Coordinate(3, 4), null), new Trip(new Coordinate(1, 2), new Coordinate(5, 6)), 49)
    ),

    BEAN(
      Reflector.ofClass(FlatBean.class),
      new FlatBean("John", "Acme", 3, 4, "BMW;3", 1, 2, 5, 6, 49),
      new FlatBean(null, "Acme", 3, 4, null, 1, 2, 5, 6, 49)
    ),

    CUSTOM(
      Reflector.custom(
        Flat.class,
        row -> new Flat(row.getString(0), row.getString(1), row.getInt(2), row.getInt(3), row.getString(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8), row.getInt(9)),
        List.of(
          Mapping.of("name", String.class, Flat::name),
          Mapping.of("company_name", String.class, Flat::companyName),
          Mapping.of("company_location_x", int.class, Flat::companyLocationX),
          Mapping.of("company_location_y", int.class, Flat::companyLocationY),
          Mapping.of("company_car", String.class, Flat::companyCar),
          Mapping.of("trip_start_x", int.class, Flat::tripStartX),
          Mapping.of("trip_start_y", int.class, Flat::tripStartY),
          Mapping.of("trip_end_x", int.class, Flat::tripEndX),
          Mapping.of("trip_end_y", int.class, Flat::tripEndY),
          Mapping.of("age", int.class, Flat::age)
        )
      ),
      new Flat("John", "Acme", 3, 4, "BMW;3", 1, 2, 5, 6, 49),
      new Flat(null, "Acme", 3, 4, null, 1, 2, 5, 6, 49)
    ),

    PARTIAL_FLAT(
      Reflector.of(PartialFlat.class)
        .inline("company_location", Reflector.of(Coordinate.class).prefix("company_location_")),
      new PartialFlat("John", "Acme", new Coordinate(3, 4), "BMW;3", 1, 2, 5, 6, 49),
      new PartialFlat(null, "Acme", new Coordinate(3, 4), null, 1, 2, 5, 6, 49)
    ),

    CUSTOM_INLINE(
      Reflector.custom(
        PartialFlat.class,
        row -> new PartialFlat(row.getString(0), row.getString(1), row.getObject(2, Coordinate.class), row.getString(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8)),
        List.of(
          Mapping.of("name", String.class, PartialFlat::name),
          Mapping.of("company_name", String.class, PartialFlat::companyName),
          Mapping.of("company_location", Coordinate.class, PartialFlat::companyLocation),
          Mapping.of("company_car", String.class, PartialFlat::companyCar),
          Mapping.of("trip_start_x", int.class, PartialFlat::tripStartX),
          Mapping.of("trip_start_y", int.class, PartialFlat::tripStartY),
          Mapping.of("trip_end_x", int.class, PartialFlat::tripEndX),
          Mapping.of("trip_end_y", int.class, PartialFlat::tripEndY),
          Mapping.of("age", int.class, PartialFlat::age)
        )
      )
      .nest("company_location", Reflector.of(Coordinate.class)),
      new PartialFlat("John", "Acme", new Coordinate(3, 4), "BMW;3", 1, 2, 5, 6, 49),
      new PartialFlat(null, "Acme", new Coordinate(3, 4), null, 1, 2, 5, 6, 49)
    ),

    CUSTOM_WITH_MANUAL_INLINE(
      Reflector.custom(
        PartialFlat.class,
        row -> new PartialFlat(row.getString(0), row.getString(1), row.getObject(2, Coordinate.class), row.getString(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8)),
        List.of(
          Mapping.of("name", String.class, PartialFlat::name),
          Mapping.of("company_name", String.class, PartialFlat::companyName),
          Mapping.inline(PartialFlat::companyLocation, Reflector.custom(
            Coordinate.class,
            row -> new Coordinate(row.getInt(0), row.getInt(1)),
            List.of(
              Mapping.of("company_location_x", int.class, Coordinate::x),
              Mapping.of("company_location_y", int.class, Coordinate::y)
            )
          )),
          Mapping.of("company_car", String.class, PartialFlat::companyCar),
          Mapping.of("trip_start_x", int.class, PartialFlat::tripStartX),
          Mapping.of("trip_start_y", int.class, PartialFlat::tripStartY),
          Mapping.of("trip_end_x", int.class, PartialFlat::tripEndX),
          Mapping.of("trip_end_y", int.class, PartialFlat::tripEndY),
          Mapping.of("age", int.class, PartialFlat::age)
        )
      ),
      new PartialFlat("John", "Acme", new Coordinate(3, 4), "BMW;3", 1, 2, 5, 6, 49),
      new PartialFlat(null, "Acme", new Coordinate(3, 4), null, 1, 2, 5, 6, 49)
    );

    final Reflector<Object> reflector;
    final Object testObject;
    final Object nullTestObject;

    @SuppressWarnings("unchecked")
    <T> ReflectorCase(Reflector<T> reflector, T testObject, T nullTestObject) {
      this.reflector = (Reflector<Object>)reflector;
      this.testObject = testObject;
      this.nullTestObject = nullTestObject;
    }
  }

  @Nested
  class WhenReflectorCreated {

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldExcludeFields(ReflectorCase c) {
      assertThat(c.reflector.excluding("company_location_x", "company_location_y").names())
        .isEqualTo(List.of("name", "company_name", "", "", "company_car", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y", "age"));
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldRejectExcludingNonExistingFields(ReflectorCase c) {
      assertThatThrownBy(() -> c.reflector.excluding("company_location_x", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("unable to exclude non-existing fields: [b], available are: [name, company_name, company_location_x, company_location_y, company_car, trip_start_x, trip_start_y, trip_end_x, trip_end_y, age]");
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldKeepFields(ReflectorCase c) {
      assertThat(c.reflector.only("company_location_x", "company_location_y").names())
        .isEqualTo(List.of("", "", "company_location_x", "company_location_y", "", "", "", "", "", ""));
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

      assertThat(baseValues.names()).isEqualTo(List.of("name", "company_name", "company_location_x", "company_location_y", "company_car", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y", "age"));
      assertThat(baseValues.getValue(0, 0)).isEqualTo("John");
      assertThat(baseValues.getValue(0, 1)).isEqualTo("Acme");
      assertThat(baseValues.getValue(0, 2)).isEqualTo(3);
      assertThat(baseValues.getValue(0, 3)).isEqualTo(4);
      assertThat(baseValues.getValue(0, 4)).isEqualTo("BMW;3");
      assertThat(baseValues.getValue(0, 5)).isEqualTo(1);
      assertThat(baseValues.getValue(0, 6)).isEqualTo(2);
      assertThat(baseValues.getValue(0, 7)).isEqualTo(5);
      assertThat(baseValues.getValue(0, 8)).isEqualTo(6);
      assertThat(baseValues.getValue(0, 9)).isEqualTo(49);
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldCreateEntries(ReflectorCase c) {
      Entries baseValues = c.reflector.entries(c.testObject);

      assertThat(baseValues.names()).isEqualTo(List.of("name", "company_name", "company_location_x", "company_location_y", "company_car", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y", "age"));
      assertThat(baseValues.getValue(0, 0)).isEqualTo("John");
      assertThat(baseValues.getValue(0, 1)).isEqualTo("Acme");
      assertThat(baseValues.getValue(0, 2)).isEqualTo(3);
      assertThat(baseValues.getValue(0, 3)).isEqualTo(4);
      assertThat(baseValues.getValue(0, 4)).isEqualTo("BMW;3");
      assertThat(baseValues.getValue(0, 5)).isEqualTo(1);
      assertThat(baseValues.getValue(0, 6)).isEqualTo(2);
      assertThat(baseValues.getValue(0, 7)).isEqualTo(5);
      assertThat(baseValues.getValue(0, 8)).isEqualTo(6);
      assertThat(baseValues.getValue(0, 9)).isEqualTo(49);
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldCreateNullValues(ReflectorCase c) {
      Values baseValues = c.reflector.values(c.nullTestObject);

      assertThat(baseValues.names()).isEqualTo(List.of("name", "company_name", "company_location_x", "company_location_y", "company_car", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y", "age"));
      assertThat(baseValues.getValue(0, 0)).isNull();
      assertThat(baseValues.getValue(0, 1)).isEqualTo("Acme");
      assertThat(baseValues.getValue(0, 2)).isEqualTo(3);
      assertThat(baseValues.getValue(0, 3)).isEqualTo(4);
      assertThat(baseValues.getValue(0, 4)).isNull();
      assertThat(baseValues.getValue(0, 5)).isEqualTo(1);
      assertThat(baseValues.getValue(0, 6)).isEqualTo(2);
      assertThat(baseValues.getValue(0, 7)).isEqualTo(5);
      assertThat(baseValues.getValue(0, 8)).isEqualTo(6);
      assertThat(baseValues.getValue(0, 9)).isEqualTo(49);
    }

    @ParameterizedTest
    @EnumSource(ReflectorCase.class)
    void shouldCreateNullEntries(ReflectorCase c) {
      Entries baseValues = c.reflector.entries(c.nullTestObject);

      assertThat(baseValues.names()).isEqualTo(List.of("name", "company_name", "company_location_x", "company_location_y", "company_car", "trip_start_x", "trip_start_y", "trip_end_x", "trip_end_y", "age"));
      assertThat(baseValues.getValue(0, 0)).isNull();
      assertThat(baseValues.getValue(0, 1)).isEqualTo("Acme");
      assertThat(baseValues.getValue(0, 2)).isEqualTo(3);
      assertThat(baseValues.getValue(0, 3)).isEqualTo(4);
      assertThat(baseValues.getValue(0, 4)).isNull();
      assertThat(baseValues.getValue(0, 5)).isEqualTo(1);
      assertThat(baseValues.getValue(0, 6)).isEqualTo(2);
      assertThat(baseValues.getValue(0, 7)).isEqualTo(5);
      assertThat(baseValues.getValue(0, 8)).isEqualTo(6);
      assertThat(baseValues.getValue(0, 9)).isEqualTo(49);
    }

    @Nested
    class AndARowNeedsMapping {

      @ParameterizedTest
      @EnumSource(ReflectorCase.class)
      void shouldCreateRecord(ReflectorCase c) {
        Row row = Row.of("John", "Acme", 3, 4, "BMW;3", 1, 2, 5, 6, 49);

        assertThat(c.reflector.apply(row)).isEqualTo(c.testObject);
      }

      @ParameterizedTest
      @EnumSource(ReflectorCase.class)
      void shouldCreateRecordWithNulls(ReflectorCase c) {
        Row row = Row.of(null, "Acme", 3, 4, null, 1, 2, 5, 6, 49);

        assertThat(c.reflector.apply(row)).isEqualTo(c.nullTestObject);
      }
    }
  }

  public static class FlatBean {
    private String name;
    private String companyName;
    private int companyLocationX;
    private int companyLocationY;
    private String companyCar;
    private int tripStartX;
    private int tripStartY;
    private int tripEndX;
    private int tripEndY;
    private int age;

    public FlatBean(String name, String companyName, int companyLocationX, int companyLocationY, String companyCar, int tripStartX, int tripStartY, int tripEndX, int tripEndY, int age) {
      this.name = name;
      this.companyName = companyName;
      this.companyLocationX = companyLocationX;
      this.companyLocationY = companyLocationY;
      this.companyCar = companyCar;
      this.tripStartX = tripStartX;
      this.tripStartY = tripStartY;
      this.tripEndX = tripEndX;
      this.tripEndY = tripEndY;
      this.age = age;
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

    public String getCompanyCar() {
      return companyCar;
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

    public int getAge() {
      return age;
    }

    @Override
    public int hashCode() {
      return Objects.hash(companyLocationX, companyLocationY, companyName, companyCar, name, tripEndX, tripEndY, tripStartX, tripStartY, age);
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if(obj == null || getClass() != obj.getClass()) {
        return false;
      }

      FlatBean other = (FlatBean)obj;

      return companyLocationX == other.companyLocationX && companyLocationY == other.companyLocationY
          && Objects.equals(companyName, other.companyName) && Objects.equals(name, other.name)
          && Objects.equals(companyCar, other.companyCar)
          && tripEndX == other.tripEndX && tripEndY == other.tripEndY
          && tripStartX == other.tripStartX && tripStartY == other.tripStartY
          && age == other.age;
    }
  }

  public record Flat(String name, String companyName, int companyLocationX, int companyLocationY, String companyCar, int tripStartX, int tripStartY, int tripEndX, int tripEndY, int age) {}
  public record PartialFlat(String name, String companyName, Coordinate companyLocation, String companyCar, int tripStartX, int tripStartY, int tripEndX, int tripEndY, int age) {}
  private record Employee(String name, Company company, Trip trip, int age) {}
  public record Employee2(String naam, Company bedrijf, Trip trip, int leeftijd) {}
  public record Trip(Coordinate start, Coordinate end) {}
  public record Company(String name, Coordinate location, Car car) {}
  public record Coordinate(int x, int y) {}
  public record Car(String brand, String type) {}
}
