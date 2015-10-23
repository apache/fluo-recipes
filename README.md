# Fluo Recipes

[![Build Status](https://travis-ci.org/fluo-io/fluo-recipes.svg?branch=master)](https://travis-ci.org/fluo-io/fluo-recipes)

Common code for Fluo application developers.  

### Available Recipes

* [Export Queue][export-q] - A recipe for exporting data from Fluo to external systems.
* [RecordingTransaction][recording-tx] - A wrapper for a Fluo transaction that records all transaction
operations to a log which can be used to export data from Fluo.

### Common Functionality

Recipes have common needs that are broken down into the following reusable components.

* [Serialization][serialization] - Common code for serializing POJOs. 
* [Transient Ranges][transient] - Standardized process for dealing with transient data.
* [Table optimization][optimization] - Standardized process for optimizing the Fluo table.

[export-q]:docs/export-queue.md
[recording-tx]: docs/recording-tx.md
[serialization]: docs/serialization.md
[transient]: docs/transient.md
[optimization]: docs/table-optimization.md
