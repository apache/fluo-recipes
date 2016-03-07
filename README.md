# Fluo Recipes

[![Build Status](https://travis-ci.org/fluo-io/fluo-recipes.svg?branch=master)](https://travis-ci.org/fluo-io/fluo-recipes)

Common code for Fluo application developers.  

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

[cfm]:docs/cfm.md
[export-q]:docs/export-queue.md
[recording-tx]: docs/recording-tx.md
[serialization]: docs/serialization.md
[transient]: docs/transient.md
[optimization]: docs/table-optimization.md
[row-hasher]: docs/row-hasher.md
[testing]: docs/testing.md
