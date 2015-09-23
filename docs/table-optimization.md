# Fluo Table Optimization

## Background

Recipes may need to make Accumulo specific table modification for optimal
performance.  Configuring the Accumulo tablet balancer and adding splits are
two optimizations that are currently done.  Offering a standard way to do these
optimizations makes it easier to use recipes correctly.  These optimizations
are optional.  You could skip them for integration testing, but would probably
want to use them in production.

## Example

```java

FluoConfiguration fluoConf = ...

//Post initialization table optimizations
Pirtos pirtos = new Pirtos();

//export queue configure method will return table optimizations it would like made
pirtos.merge(ExportQueue.configure(fluoConf, ...));

//CollisionFreeMap.configure() will return table optimizations it would like made
pritos.merge(CollisionFreeMap.configure(fluoConf, ...));

//initialize Fluo
FluoFactory.newAdmin(fluoConf).initialize(...)

//perform table optimizations
TableOperations.optimizeTable(fluoConf, pirtos);

```
