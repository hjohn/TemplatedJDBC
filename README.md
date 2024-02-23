[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.int4.db/parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.int4.db/parent)
[![Build Status](https://github.com/hjohn/TemplatedJDBC/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/hjohn/TemplatedJDBC/actions)
[![Coverage](https://codecov.io/gh/hjohn/TemplatedJDBC/branch/master/graph/badge.svg?token=QCNNRFYF98)](https://codecov.io/gh/hjohn/TemplatedJDBC)
[![License](https://img.shields.io/badge/License-BSD_2--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![javadoc](https://javadoc.io/badge2/org.int4.db/parent/javadoc.svg)](https://javadoc.io/doc/org.int4.db/parent)

# Name

A zero dependency light-weight wrapper for JDBC, using String Templates and records.

# Introduction

This framework uses the String Templates preview feature. In order to use it you must use Java 21 or higher with preview features enabled (`--enable-preview`). This is still very much an experimental framework to see what a convenient syntax would be for a light-weight JDBC wrapper, and as such is subject to change.

It leans heavily on immutable `record`s, and is assuming there will be a future convenient syntax for reconstructing records with different values. Until that time, custom `with`-ers, or a framework like [Record Builder](https://github.com/Randgalt/record-builder) can be used to alleviate some of these discomforts.

It is extensible enough to allow using types that are not `record`s, although currently there is limited support to make this usage easy and convenient. A custom solution that generates the necessary meta data using reflection and annotations may be included at a later time; until that time it is possible to provide your own (or make a pull request).

## Activating Preview Features with Maven

For the compiler plugin, add:

```xml
<configuration>
    <enablePreview>true</enablePreview>
</configuration>
```

For the surefire and failsafe plugins add:

```xml
<configuration>
    <argLine>--enable-preview</argLine>
</configuration>
```

# Quick Example

```java
class Example {
    record Employee(Integer id, String name, double salary) {}

    // Create a Reflector to hold Java to Database name mapping information:
    static final Reflector ALL = Reflector.of(Employee.class);

    final Database database;

    // Obtain DataSource from HikariCP for example
    Example(DataSource dataSource) {
        // Database is reusable, and can be shared for a whole application
        // so this can also be provided as a constructor parameter instead
        // of creating it here:
        this.database = new Database(dataSource::getConnection);
    }

    // Generates: "INSERT INTO employee (id, name, salary) VALUES (?, ?, ?)"
    void insertExample() {
        Employee employee = new Employee(1, "Jane", 1000.0);

        // Transaction is an AutoCloseable, so can use try with resources:
        try (Transaction tx = db.beginTransaction()) {
            // Execute an insert:
            tx."INSERT INTO employee (\{ALL}) VALUES (\{employee})"
                .execute();  // Execute and return row count

            // Commit the transaction (default is to rollback):
            tx.commit();
        }
    }

    // Generates: "SELECT e.id, e.name, e.salary FROM employee e"
    List<Employee> queryExample() {
        try (Transaction tx = db.beginReadOnlyTransaction()) {
            return tx."SELECT e.\{ALL} FROM employee e"
                .map(ALL)  // Convert to Employee records
                .asList();  // Execute and return a List
        }
    }

    // Generates: "UPDATE employee SET id = ?, name = ?, salary = ? WHERE id = ?"
    long updateExample() {
        try (Transaction tx = db.beginTransaction()) {
            Employee employee = new Employee(1, "Jane", 1000.0);

            long rowCount = tx."""
                UPDATE employee SET \{ALL.entries(employee)}
                    WHERE id = \{employee.id()}
            """.execute();

            tx.commit();

            return rowCount;
        }
    }
}
```
For more examples see `DatabaseIT.java`.

# The Basics

## Transactions

Transaction are `AutoCloseable` making it possible to use them with a try with resources construct. Transactions are associated with a thread, and starting a nested transaction will create a save point in the top level transaction to which a nested transaction may be rolled back.

Transactions when closed will by default perform a rollback, unless `Transaction::commit` is called.

```java
try (Transaction tx = db.beginTransaction()) {
    // query or modify database here

    // when done commit (or do nothing for rollback)
    tx.commit();
}
```

In further examples, `tx` will hold an active `Transaction`.

## SQL execution

Simple SQL statements which require no template parameters can be executed directly:

```java
List<Row> employees = tx."SELECT * FROM employee".toList();
List<Double> salaries = tx."SELECT salary FROM employee".asDouble().toList();
int count = tx."SELECT COUNT(*) FROM employee".map(r -> r.getInteger()).get();
long affectedRows = tx."UPDATE employee SET salary = salary * 2".execute();
```

The template allows several customizations before the statement is executed. Depending on the statement type, the template can be executed immediately to get the number of affected rows, or it can be treated as a query in which case it will return `Row`s unless mapped to a more specific type. Only terminal operations will trigger the execution of the statement.

|Method Signature|Operation Type|Description|
|---|---|---|
|`map(Function)`|Intermediate|Maps from the current type `T` to a new type `U`|
|`mapGeneratedKeys`|Intermediate|Switches to the `Row`s that contain the generated keys|
|`asInt`, `asLong`, ...|Intermediate|Assumes a single column, and maps the first column to the indicated type|
|`execute`|Terminal Statement|Executes the statement and returns the number of affected rows|
|`get`|Terminal Query|Executes the statement and returns the first and only result, or `null` if unavailable|
|`toOptional`|Terminal Query|Executes the statement and returns the first and only result as an `Optional<T>`|
|`toList`|Terminal Query|Executes the statement and returns the results as a (possibly empty) `List`|
|`consume(Consumer)`|Terminal Query|Executes the statement, and calls the given consumer for each type `T`|
|`consume(Consumer, long)`|Terminal Query|Executes the statement, and calls the given consumer for each type `T` up to a given maximum|

> Note: `Row`s are thin wrapper around a JDBC `ResultSet` but use zero based column indices to fit in better.

## Using template parameters
```java
String name = "John"

int count = tx."SELECT COUNT(*) FROM employee WHERE name = \{name}".asInt().get();
```
> Resulting SQL: `SELECT COUNT(*) FROM employee WHERE name = ?`

# Leveraging `record`s

Handling data using untyped `array`s is cumbersome and error prone. Instead, `record`s can be used to handle groups of fields as a single unit.

In its most simple form, a `record` can be used as a list of values. Given:

```java
record Employee(Integer id, String name, double salary);
```
Then the record can be leveraged as:
```java
Record employee = new Employee(5, "Jane", 1000.0);

tx."INSERT INTO employee (id, name, salary) VALUES (\{employee})".execute();
```
> SQL: `INSERT INTO employee (id, name, salary) VALUES (?, ?, ?)`

The above statement will take the required values directly form the record. Note that this is still error prone as the order of the fields must match those of the record.

## Introducing `Reflector`s, `Mapper`s and `Extractor`s

|Type|Purpose|
|---|---|
|`Mapper`|Takes a `Row` from a database and converts it a Java object|
|`Extractor`|Provides fields or fields + values to `INSERT`, `UPDATE` or `SELECT` statements|
|`Reflector`|The combination of a `Mapper` and `Extractor`|

A `Reflector` can be obtained by providing it with a suitable record definition. The record must use primitive types supported by the underlying database, but can include nested record types consisting on only supported types.

> `Reflector`s can also be created for arbitrary types, although this involves providing both the means to create such a type, and a way to extract values from it. At the moment there is only limited support for such types, and some manual work is needed. In the future it's possible that Reflection + Annotations may make this task a bit easier. For now it is assumed we're dealing with record types.

By default, the component names of a `record` are translated by converting a camel case name to an underscored name. For example, `creationDate` would become `creation_date`. The names can be overridden by calling additional methods on the `Reflector`.

`Reflector`s and `Extractor`s can be used as template parameters. Given the following static definitions:

```java
static final Reflector<Employee> ALL = Reflector.of(Employee.class);
static final Extractor<Employee> EXCEPT_ID = ALL.excluding("id");
```
The table below shows the generated SQL for all possible usages of these types:

|Template Parameter|Templated as|SQL|
|---|---|---|
|`\{ALL}`|Field List|`id, name, salary`|
|`\{EXCEPT_ID}`||`name, salary`|
|`e.\{EXCEPT_ID}` (with alias)||`e.name, e.salary`|
|||
|`\{ALL.values(employee)}` or `\{employee}`|Value List|`?, ?, ?` (id, name, salary)|
|`\{EXCEPT_ID.values(employee)}`||`?, ?` (name, salary)|
|||
|`\{EXCEPT_ID.entries(employee)}`|Key/Value List|`name = ?, salary = ?`|

> Note: `record`s that are provided directly as template parameters must have their components defined in the exact same order as the supplied fields. No attempt is made to match fields by name.

## Examples

```java
// Reading records
List<Employee> = tx."SELECT \{ALL} FROM employee WHERE id > 2"
    .map(ALL)  // The ALL Reflector is also a Mapper
    .toList();
```
> SQL: `SELECT id, name, salary FROM employee WHERE id > 2`
```java
// Inserting records
Employee newEmployee = new Employee(null, "John", 42.42);

tx."INSERT INTO employee (\{EXCEPT_ID}) VALUES (\{newEmployee})".execute();
```
> SQL: `INSERT INTO employee (name, salary) VALUES (?, ?)`
```java
// Updating records
Employee updatedEmployee = newEmployee.withName("Jane");

tx."UPDATE employee SET \{EXCEPT_ID.entries(updatedEmployee)} WHERE id = 5".execute();
```
> SQL: `UPDATE employee SET name = ?, salary = ? WHERE id = 5`

## Nested Records

`Reflector`s support nested records and will flatten such records to a single set of fields. Given:

```java
record Company(Integer id, String name, Address address) {}
record Address(String street, String city) {}
```
A `Reflector` created for `Company` will have the fields "id", "name", "street" and "city". A query that joins an address can then be directly mapped to a `Company` record:

```java
Reflector<Company> COMPANY = Reflector.of(Company.class);

Company c = tx."SELECT \{COMPANY} FROM company c JOIN address a WHERE a.id = c.id"
    .map(COMPANY)
    .get();
```

## Obtaining and using generated ID's

Tables are often defined with id's generated by the database. In order to obtain these and (re)construct the resulting `record` with the correct id, use the following form for an `INSERT` statement:

```java
Employee employee = new Employee(null, "John", 42.42);

Employee employeeWithId = tx."INSERT INTO employee (\{ALL}) VALUES (\{employee})"
    .mapGeneratedKeys()  // switch to the generated keys result set
    .asInt()  // map first column to integers
    .map(employee::withId)  // reconstruct the record using a custom wither
    .get();  // executes the statement and returns the new record
```

# Advanced Stuff

In JDBC, normally only parameters can be templated, which is very limited. In this framework, the templates recognize several types that it can integrate safely into an SQL statement. This is done by strictly limiting what these types can provide.

|Type|Use|
|---|---|
|Any `Record` type|As JDBC placeholders|
|`Extractor`|As field names|
|`Extractor::entries`|A combination of names and JDBC placeholders suitable for `UPDATE` statements|
|`Extractor::values`|As JDBC placeholders, possibly filtered|
|`Identifier`|As an SQL identifier|

## `Identifier`s

Sometimes it is needed to be able to template more than just fields and values. A system that can create tables on demand, and query from them, may need to be able to specify its own table name. It is not possible to concatenate such information into a String Template, as templates do not allow concatenation for security reasons, nor does the template engine in this framework allow for inclusion of arbitrary `String`s.

In order to support this use case, one can convert a `String` into an `Identifier`. This involves a strict validation check that ensures the `String` can only be an SQL identifier, and thus would be safe to include. This looks like this:

```java
tx."CREATE TABLE \{Identifier.of("my_table")} ( ... )".execute();
```

# Checked Exceptions, or not...

You choose. If you create the `Transaction` object using `beginCheckedTransaction`, then all operations will throw the well known `SQLException` for you to propagate. If you use `beginTransaction`, then all operations will throw an unchecked `DatabaseException`.

# Future

- Batch support, by providing a `Collection` to appropriate statements
- More investigating how to support aliasing of fields