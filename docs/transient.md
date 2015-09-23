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

A process could be started that calls this every 15 minutes or 30 minutes.

[1]:../modules/core/src/main/java/io/fluo/recipes/common/TransientRegistry.java
[2]:../modules/accumulo/src/main/java/io/fluo/recipes/accumulo/ops/TableOperations.java
