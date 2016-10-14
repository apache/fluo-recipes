<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Apache Fluo Recipes

[![Build Status][ti]][tl] [![Apache License][li]][ll]

**Fluo Recipes are common code for [Apache Fluo][fluo] application developers.**

Fluo Recipes build on the [Fluo API][fluo-api] to offer additinal functionality to
developers. They are published seperately from Fluo on their own release schedule.
This allows Fluo Recipes to iterate and innovate faster than Fluo (which will maintain
a more minimal API on a slower release cycle).

### Documentation

Recipes are documented below and in the [Recipes API docs][recipes-api].

* [Collision Free Map][cfm] - A recipe for making many to many updates.
* [Export Queue][export-q] - A recipe for exporting data from Fluo to external systems.
* [Row Hash Prefix][row-hasher] - A recipe for spreading data evenly in a row prefix.
* [RecordingTransaction][recording-tx] - A wrapper for a Fluo transaction that records all transaction
operations to a log which can be used to export data from Fluo.
* [Testing][testing] Some code to help write Fluo Integration test.

Recipes have common needs that are broken down into the following reusable components.

* [Serialization][serialization] - Common code for serializing POJOs. 
* [Transient Ranges][transient] - Standardized process for dealing with transient data.
* [Table optimization][optimization] - Standardized process for optimizing the Fluo table.

### Usage

The Fluo Recipes project publishes multiple jars to Maven Central for each release.
The `fluo-recipes-core` jar is the primary jar. It is where most recipes live and where
they are placed by default if they have minimal dependencies beyond the Fluo API.

Fluo Recipes with dependencies that bring in many transitive dependencies publish
their own jar. For example, recipes that depend on Apache Spark are published in the
`fluo-recipes-spark` jar.  If you don't plan on using code in the `fluo-recipes-spark`
jar, you should avoid including it in your pom.xml to avoid a transitive dependency on
Spark.

Below is a sample Maven POM containing all possible Fluo Recipes dependencies:

```xml
  <properties>
    <fluo-recipes.version>1.0.0-incubating</fluo-recipes.version>
  </properties>

  <dependencies>
    <!-- Required. Contains recipes that are only depend on the Fluo API -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-core</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!-- Optional. Serialization code that depends on Kryo -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-kryo</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!-- Optional. Common code for using Fluo with Accumulo -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-accumulo</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!-- Optional. Common code for using Fluo with Spark -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-spark</artifactId>
      <version>${fluo-recipes.version}</version>
    </dependency>
    <!-- Optional. Common code for writing Fluo integration tests -->
    <dependency>
      <groupId>org.apache.fluo</groupId>
      <artifactId>fluo-recipes-test</artifactId>
      <version>${fluo-recipes.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
```

[fluo]: https://fluo.apache.org/
[fluo-api]: https://fluo.apache.org/apidocs/fluo/
[recipes-api]: https://fluo.apache.org/apidocs/fluo-recipes/
[cfm]: docs/cfm.md
[export-q]: docs/export-queue.md
[recording-tx]: docs/recording-tx.md
[serialization]: docs/serialization.md
[transient]: docs/transient.md
[optimization]: docs/table-optimization.md
[row-hasher]: docs/row-hasher.md
[testing]: docs/testing.md
[ti]: https://travis-ci.org/apache/incubator-fluo-recipes.svg?branch=master
[tl]: https://travis-ci.org/apache/incubator-fluo-recipes
[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://github.com/apache/incubator-fluo-recipes/blob/master/LICENSE
