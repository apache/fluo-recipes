# Testing

Fluo includes MiniFluo which makes it possible to write an integeration test that
runs against a real Fluo instance.  Fluo Recipes provides the following utility
code for writing an integration test.

 * [FluoITHelper][1] A class with utility methods for comparing expected data with whats in Fluo.
 * [AccumuloExportITBase][2] A base class for writing an integration test that exports data from Fluo to an external Accumulo table.

[1]:../modules/test/src/main/java/io/fluo/recipes/test/FluoITHelper.java
[2]:../modules/test/src/main/java/io/fluo/recipes/test/AccumuloExportITBase.java
