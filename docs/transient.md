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
# Transient data

## Background

Some recipes store transient data in a portion of the Fluo table.  Transient
data is data thats continually being added and deleted.  Also these transient
data ranges contain no long term data.  The way Fluo works, when data is
deleted a delete marker is inserted but the data is actually still there.  Over
time these transient ranges of the table will have a lot more delete markers
than actual data if nothing is done.  If nothing is done, then processing
transient data will get increasingly slower over time.

These deleted markers can be cleaned up by forcing Accumulo to compact the
Fluo table, which will run Fluos garbage collection iterator. However,
compacting the entire table to clean up these ranges within a table is
overkill. Alternatively,  Accumulo supports compacting ranges of a table.   So
a good solution to the delete marker problem is to periodically compact just
the transient ranges. 

Fluo Recipes provides helper code to deal with transient data ranges in a
standard way.

## Registering Transient Ranges

Recipes like [Export Queue](export-queue.md) will automatically register
transient ranges when configured.  If you would like to register your own
transient ranges, use [TransientRegistry][1].  Below is a simple example of
using this.

```java
FluoConfiguration fluoConfig = ...;
TransientRegistry transientRegistry = new TransientRegistry(fluoConfig.getAppConfiguration());
transientRegistry.addTransientRange(new RowRange(startRow, endRow));

//Initialize Fluo using fluoConfig. This will store the registered ranges in
//zookeeper making them availiable on any node later.
```

## Compacting Transient Ranges

Although you may never need to register transient ranges directly, you will
need to periodically compact transient ranges if using a recipe that registers
them.  Using [TableOperations][2] this can be done with one line of Java code
like the following.

```java
FluoConfiguration fluoConfig = ...;
TableOperations.compactTransient(fluoConfig);
```

Fluo recipes provides an easy way to compact transient ranges from the command line using the `fluo exec` command as follows:

```
fluo exec <app name> org.apache.fluo.recipes.accumulo.cmds.CompactTransient [<interval> [<multiplier>]]
```

If no arguments are specified the command will call `compactTransient()` once.
If `<interval>` is specified the command will run forever compacting transient
ranges sleeping `<interval>` seconds between compacting each transient ranges.

In the case where Fluo is backed up in processing data a transient range could
have a lot of data queued and compacting it too frequently would be
counterproductive.  To avoid this the `CompactTransient` command will consider
the time it took to compact a range when deciding when to compact that range
next.  This is where the `<multiplier>` argument comes in, the time to sleep
between compactions of a range is determined as follows.  If not specified, the
multiplier defaults to 3.

```java
   sleepTime = Math.max(compactTime * multiplier, interval);
```

For example assume a Fluo application has two transient ranges.  Also assume
CompactTransient is run with an interval of 600 and a multiplier of 10.  If the
first range takes 20 seconds to compact, then it will be compacted again in 600
seconds.  If the second range takes 80 seconds to compact, then it will be
compacted again in 800 seconds.

[1]: ../modules/core/src/main/java/org/apache/fluo/recipes/core/common/TransientRegistry.java
[2]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/ops/TableOperations.java
