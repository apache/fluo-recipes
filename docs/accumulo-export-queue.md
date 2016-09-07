# Accumulo Export Queue Specialization

## Background

The [Export Queue Recipe][1] provides a generic foundation for building export mechanism to any
external data store. The [AccumuloExportQueue] provides an implementation of this recipe for
Accumulo. The [AccumuloExportQueue] is located the 'fluo-recipes-accumulo' module and provides the
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
      protected Collection<Mutation> processExport(SequencedExport<String, String> export) {
        Mutation m = new Mutation(export.getKey());
        m.put("cf", "cq", export.getSequence(), export.getValue());
        return Collections.singleton(m);
      }
    }
    ```

2. With a `SimpleExporter` created, configure a [AccumuloExportQueue] to use `SimpleExporter` and
   give it information on how to connect to Accumulo. 

    ```java

    FluoConfiguration fluoConfig = ...;

    // Set accumulo configuration
    String instance =       // Name of accumulo instance exporting to
    String zookeepers =     // Zookeepers used by Accumulo instance exporting to
    String user =           // Accumulo username, user that can write to exportTable
    String password =       // Accumulo user password
    String exportTable =    // Name of table to export to

    // Configure accumulo export queue
    AccumuloExportQueue.configure(fluoConfig, new ExportQueue.Options(EXPORT_QUEUE_ID,
        SimpleExporter.class.getName(), String.class.getName(), String.class.getName(), numMapBuckets),
        new AccumuloExportQueue.Options(instance, zookeepers, user, password, exportTable));

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
[AccumuloExportQueue]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/export/AccumuloExportQueue.java
[AccumuloExporter]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/export/AccumuloExporter.java
[AccumuloReplicator]: ../modules/accumulo/src/main/java/org/apache/fluo/recipes/accumulo/export/DifferenceExport.java

