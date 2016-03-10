# Accumulo Export Queue Specialization

## Background

The [Export Queue Recipe][1] provides a generic foundation for building export 
mechanism to any external data store.  A specific Export Queue implementation
for Accumulo is provided in the Fluo Recipes Accumulo module.

This implementation provides the following functionality :

 * Safely batches writes to Accumulo made by multiple transactions exporting data.
 * Stores Accumulo connection information in Fluo configuration, making it accessible by Export Observers running on other nodes.
 * Provides utility code that make it easier and shorter to code common Accumulo export patterns.

## Example Use

Exporting to Accumulo is easy.  Only two things need to be done.

 * Configure the export queue.
 * Implement [AccumuloExport][2] with custom code that generates Accumulo mutations.

The following code shows how to configure an Export Queue that will write to an
external Accumulo table.

```java

FluoConfiguration fluoConfig = ...;

//Configure an export queue to use the classes Fluo Recipes provides for
//exporting to Accumulo
ExportQueue.configure(fluoConfig, new ExportQueue.Options(EXPORT_QUEUE_ID,
    AccumuloExporter.class.getName(), String.class.getName(), AccumuloExport.class.getName(),
    numMapBuckets));

String instance = //Name of accumulo instance exporting to
String zookeepers = //zookeepers used by Accumulo instance exporting to
String user = //Accumulo username, user that can write to exportTable
String password = //Accumulo user password
String exportTable = //Name of table to export to

//Configure the Accumulo table to export to.
AccumuloExporter.setExportTableInfo(fluoConfig, EXPORT_QUEUE_ID,
    new TableInfo(instance, zookeepers, user, password, exportTable));

//initialize Fluo using fluoConfig

```

After the export queue is initialized as specified above, any Object that
implements [AccumuloExport][2] can be added to the queue.  For the common
pattern of deleting old data and inserting new data, consider extending
[DifferenceExport][3].

[1]:export-queue.md
[2]:../modules/accumulo/src/main/java/io/fluo/recipes/accumulo/export/AccumuloExport.java
[3]:../modules/accumulo/src/main/java/io/fluo/recipes/accumulo/export/DifferenceExport.java

