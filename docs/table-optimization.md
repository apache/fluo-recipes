# Fluo Table Optimization

## Background

Recipes may need to make Accumulo specific table modifications for optimal
performance.  Configuring the Accumulo tablet balancer and adding splits are
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

//initialize Fluo
FluoFactory.newAdmin(fluoConf).initialize(...)

//Automatically optimize the Fluo table for all configured recipes
Pirtos tableOptimizations = getConfiguredOptimizations(fluoConf);
TableOperations.optimizeTable(fluoConf, tableOptimizations);
```

The above example automatically optimizes all configured recipes.  If more
selective optimizations is need look into using the following methods instead.

 * `CollisionFreeMap.getTableOptimizations(String mapId, Configuration appConfig)`
 * `ExportQueue.getTableOptimizations(String queueId, Configuration appConfig)`
 * `TableOperations.optimizeTable(FluoConfiguration fluoConfig, Pirtos pirtos)`
 * `Pirtos.merge()`

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
