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
# Fluo Table Optimization

## Background

Recipes may need to make Accumulo specific table modifications for optimal
performance.  Configuring the [Accumulo tablet balancer][3] and adding splits are
two optimizations that are currently done.  Offering a standard way to do these
optimizations makes it easier to use recipes correctly.  These optimizations
are optional.  You could skip them for integration testing, but would probably
want to use them in production.

## Java Example

```java
FluoConfiguration fluoConf = ...

//export queue configure method will return table optimizations it would like made
ExportQueue.configure(fluoConf, ...);

//CollisionFreeMap.configure() will return table optimizations it would like made
CollisionFreeMap.configure(fluoConf, ...);

//configure optimizations for a prefixed hash range of a table
RowHasher.configure(fluoConf, ...);

//initialize Fluo
FluoFactory.newAdmin(fluoConf).initialize(...)

//Automatically optimize the Fluo table for all configured recipes
TableOperations.optimizeTable(fluoConf);
```

[TableOperations][2] is provided in the Accumulo module of Fluo Recipes.

## Command Example

Fluo Recipes provides an easy way to optimize a Fluo table for configured
recipes from the command line.  This should be done after configuring reciped
and initializing Fluo.  Below are example command for initializing in this way.

```bash

#create application 
fluo new app1

#configure application

#initialize Fluo
fluo init app1

#optimize table for all configured recipes
fluo exec app1 org.apache.fluo.recipes.accumulo.cmds.OptimizeTable
```

## Table optimization registry

Recipes register themself by calling [TableOptimizations.registerOptimization()][1].  Anyone can use
this mechanism, its not limited to use by exisitng recipes.

[1]: ../modules/core/src/main/java/org/apache/fluo/recipes/core/common/TableOptimizations.java
[2]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/ops/TableOperations.java
[3]: http://accumulo.apache.org/blog/2015/03/20/balancing-groups-of-tablets.html
