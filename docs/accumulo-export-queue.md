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
# Accumulo Export Queue Specialization

## Background

The [Export Queue Recipe][1] provides a generic foundation for building export mechanism to any
external data store. The [AccumuloExporter] provides an implementation of this recipe for
Accumulo. The [AccumuloExporter] is located the `fluo-recipes-accumulo` module and provides the
following functionality:

 * Safely batches writes to Accumulo made by multiple transactions exporting data.
 * Stores Accumulo connection information in Fluo configuration, making it accessible by Export
   Observers running on other nodes.
 * Provides utility code that make it easier and shorter to code common Accumulo export patterns.

## Example Use

Exporting to Accumulo is easy. Follow the steps below:

1. Implement a class that extends [AccumuloExporter].  This class will process exported objects that
   are placed on your export queue. For example, the `SimpleExporter` class below processes String
   key/value exports and generates mutations for Accumulo.

    ```java
    public class SimpleExporter extends AccumuloExporter<String, String> {

      @Override
      protected void translate(SequencedExport<String, String> export, Consumer<Mutation> consumer) {
        Mutation m = new Mutation(export.getKey());
        m.put("cf", "cq", export.getSequence(), export.getValue());
        consumer.accept(m);
      }
    }
    ```

2. With a `SimpleExporter` created, configure an `ExportQueue` to use `SimpleExporter` and
   give it information on how to connect to Accumulo. 

    ```java

    FluoConfiguration fluoConfig = ...;

    // Set accumulo configuration
    String instance =       // Name of accumulo instance exporting to
    String zookeepers =     // Zookeepers used by Accumulo instance exporting to
    String user =           // Accumulo username, user that can write to exportTable
    String password =       // Accumulo user password
    String exportTable =    // Name of table to export to


    // Create config for export table.
    AccumuloExporter.Configuration exportTableCfg =
        new AccumuloExporter.Configuration(instance, zookeepers, user, password, exportTable);

    // Create config for export queue.
    ExportQueue.Options eqOpts = new ExportQueue.Options(EXPORT_QUEUE_ID, SimpleExporter.class,
        String.class, String.class, numMapBuckets).setExporterConfiguration(exportTableCfg);

    // Configure export queue.  This will modify fluoConfig.
    ExportQueue.configure(fluoConfig, eqOpts);

    // Initialize Fluo using fluoConfig
    ```

3.  Export queues can be retrieved in Fluo observers and objects can be added to them:

    ```java
    public class MyObserver extends AbstractObserver {

      ExportQueue<String, String> exportQ;

      @Override
      public void init(Context context) throws Exception {
        exportQ = ExportQueue.getInstance(EXPORT_QUEUE_ID, context.getAppConfiguration());
      }

      @Override
      public void process(TransactionBase tx, Bytes row, Column col) {

        // Read some data and do some work

        // Add results to export queue
        String key =    // key that identifies export
        String value =  // object to export
        export.add(tx, key, value);
      }
    ```

## Other use cases

[AccumuloReplicator] is a specialized [AccumuloExporter] that replicates a Fluo table to Accumulo.

[1]: export-queue.md
[AccumuloExporter]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/export/AccumuloExporter.java
[AccumuloReplicator]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/export/AccumuloReplicator.java

