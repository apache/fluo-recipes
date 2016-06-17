# Apache Fluo Recipes

[![Build Status](https://travis-ci.org/apache/fluo-recipes.svg?branch=master)](https://travis-ci.org/apache/fluo-recipes)

Common code for [Apache Fluo][fluo] application developers.  

### Available Recipes

* [Collision Free Map][cfm] - A recipe for making many to many updates.
* [Export Queue][export-q] - A recipe for exporting data from Fluo to external systems.
* [Row Hash Prefix][row-hasher] - A recipe for spreading data evenly in a row prefix.
* [RecordingTransaction][recording-tx] - A wrapper for a Fluo transaction that records all transaction
operations to a log which can be used to export data from Fluo.
* [Testing][testing] Some code to help write Fluo Integration test.

### Common Functionality

Recipes have common needs that are broken down into the following reusable components.

* [Serialization][serialization] - Common code for serializing POJOs. 
* [Transient Ranges][transient] - Standardized process for dealing with transient data.
* [Table optimization][optimization] - Standardized process for optimizing the Fluo table.

### Modules and Maven Imports

Fluo Recipes provides multiple jars.  The primary reason its separated
into multiple jars is to avoid pushing unneeded dependencies on users.   For
example there is a Fluo Recipes spark module that depends on Spark.  If an
application does not need to use Spark, then it can easily avoid a transitive
dependency on Spark.

Below are Maven dependencies for Fluo Recipes.

```xml
  <properties>
    <fluo-recipes.version>1.0.0-beta-2</fluo-recipes.version>
  </properties>

  <dependencies>
    <!-- Recipes core only has a transitive dependency on the Fluo API -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-core</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!--optional dependency provides the default Fluo Recipes serialization
        mechanism, which is based on Kryo  -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-kryo</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!--optional dependency assist w/ intergrating Accumulo and Fluo  -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-accumulo</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!--optional dependency assist w/ intergrating Spark and Fluo -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-spark</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!--optional dependency helps when write Fluo intergeration test. -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-test</artifactId>
      <version>${fluo-recipes.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
```

[fluo]: https://fluo.apache.org/
[cfm]: docs/cfm.md
[export-q]: docs/export-queue.md
[recording-tx]: docs/recording-tx.md
[serialization]: docs/serialization.md
[transient]: docs/transient.md
[optimization]: docs/table-optimization.md
[row-hasher]: docs/row-hasher.md
[testing]: docs/testing.md
