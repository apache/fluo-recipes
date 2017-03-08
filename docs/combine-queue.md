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
# Combine Queue Recipe

## Background

When many transactions try to modify the same keys, collisions will occur.  Too many collisions
cause transactions to fail and throughput to nose dive.  For example, consider [phrasecount]
which has many transactions processing documents.  Each transaction counts the phrases in a document
and then updates global phrase counts.  Since transaction attempts to update many phrases
, the probability of collisions is high.

## Solution

The [combine queue recipe][CombineQueue] provides a reusable solution for updating many keys while
avoiding collisions.  The recipe also organizes updates into batches in order to improve throughput.

This recipes queues updates to keys for other transactions to process. In the phrase count example
transactions processing documents queue updates, but do not actually update the counts.  Below is an
example of computing phrasecounts using this recipe.

 * TX1 queues `+1` update  for phrase `we want lambdas now`
 * TX2 queues `+1` update  for phrase `we want lambdas now`
 * TX3 reads the updates and current value for the phrase `we want lambdas now`.  There is no current value and the updates sum to 2, so a new value of 2 is written.
 * TX4 queues `+2` update  for phrase `we want lambdas now`
 * TX5 queues `-1` update  for phrase `we want lambdas now`
 * TX6 reads the updates and current value for the phrase `we want lambdas now`.  The current value is 2 and the updates sum to 1, so a new value of 3 is written.

Transactions processing updates have the ability to make additional updates.
For example in addition to updating the current value for a phrase, the new
value could also be placed on an export queue to update an external database.

### Buckets

A simple implementation of this recipe would have an update queue for each key.  However the
implementation is slightly more complex.  Each update queue is in a bucket and transactions process
all of the updates in a bucket.  This allows more efficient processing of updates for the following
reasons :

 * When updates are queued, notifications are made per bucket(instead of per a key).
 * The transaction doing the update can scan the entire bucket reading updates, this avoids a seek for each key being updated.
 * Also the transaction can request a batch lookup to get the current value of all the keys being updated.
 * Any additional actions taken on update (like adding something to an export queue) can also be batched.
 * Data is organized to make reading exiting values for keys in a bucket more efficient.

Which bucket a key goes to is decided using hash and modulus so that multiple updates for a key go
to the same bucket.

The initial number of tablets to create when applying table optimizations can be controlled by
setting the buckets per tablet option when configuring a Combine Queue.  For example if you
have 20 tablet servers and 1000 buckets and want 2 tablets per tserver initially then set buckets
per tablet to 1000/(2*20)=25.

## Example Use

The following code snippets show how to use this recipe for wordcount.  The first step is to
configure it before initializing Fluo.  When initializing an ID is needed.  This ID is used in two
ways.  First, the ID is used as a row prefix in the table.  Therefore nothing else should use that
row range in the table.  Second, the ID is used in generating configuration keys.

The following snippet shows how to configure a combine queue.

```java
    FluoConfiguration fluoConfig = ...;

    // Set application properties for the combine queue.  These properties are read later by
    // the observers running on each worker.
    CombineQueue.configure(WcObserverProvider.ID)
        .keyType(String.class).valueType(Long.class).buckets(119).save(fluoConfig);

    fluoConfig.setObserverProvider(WcObserverProvider.class);

    // initialize Fluo using fluoConfig
```

Assume the following observer is triggered when a documents is updated.  It examines new
and old document content and determines changes in word counts.  These changes are pushed to a
combine queue.

```java
public class DocumentObserver implements StringObserver {
  // word count combine queue
  private CombineQueue<String, Long> wccq;

  public static final Column NEW_COL = new Column("content", "new");
  public static final Column CUR_COL = new Column("content", "current");

  public DocumentObserver(CombineQueue<String, Long> wccq) {
    this.wccq = wccq;
  }

  @Override
  public void process(TransactionBase tx, String row, Column col) {

    Preconditions.checkArgument(col.equals(NEW_COL));

    String newContent = tx.gets(row, NEW_COL);
    String currentContent = tx.gets(row, CUR_COL, "");

    Map<String, Long> newWordCounts = getWordCounts(newContent);
    Map<String, Long> currentWordCounts = getWordCounts(currentContent);

    // determine changes in word counts between old and new document content
    Map<String, Long> changes = calculateChanges(newWordCounts, currentWordCounts);

    // queue updates to word counts for processing by other transactions
    wccq.add(tx, changes);

    // update the current content and delete the new content
    tx.set(row, CUR_COL, newContent);
    tx.delete(row, NEW_COL);
  }

  private static Map<String, Long> getWordCounts(String doc) {
    // TODO extract words from doc
  }

  private static Map<String, Long> calculateChanges(Map<String, Long> newCounts,
      Map<String, Long> currCounts) {
    Map<String, Long> changes = new HashMap<>();

    // guava Maps class
    MapDifference<String, Long> diffs = Maps.difference(currCounts, newCounts);

    // compute the diffs for words that changed
    changes.putAll(Maps.transformValues(diffs.entriesDiffering(),
        vDiff -> vDiff.rightValue() - vDiff.leftValue()));

    // add all new words
    changes.putAll(diffs.entriesOnlyOnRight());

    // subtract all words no longer present
    changes.putAll(Maps.transformValues(diffs.entriesOnlyOnLeft(), l -> l * -1));

    return changes;
  }
}


```

Each combine queue has two extension points, a [combiner][Combiner] and a [change
observer][ChangeObserver].  The combine queue configures a Fluo observer to process queued
updates.  When processing updates the two extension points are called.  The code below shows
how to use these extension points.

A change observer can do additional processing when a batch of key values are updated.  Below
updates are queued for export to an external database.  The export is given the new and old value
allowing it to delete the old value if needed.

```java
public class WcObserverProvider implements ObserverProvider {

  public static final String ID = "wc";

  @Override
  public void provide(Registry obsRegistry, Context ctx) {

    ExportQueue<String, MyDatabaseExport> exportQ = createExportQueue(ctx);

    // Create a combine queue for computing word counts.
    CombineQueue<String, Long> wcMap = CombineQueue.getInstance(ID, ctx.getAppConfiguration());

    // Register observer that updates the Combine Queue
    obsRegistry.register(DocumentObserver.NEW_COL, STRONG, new DocumentObserver(wcMap));

    // Used to join new and existing values for a key. The lambda sums all values and returns
    // Optional.empty() when the sum is zero. Returning Optional.empty() causes the key/value to be
    // deleted. Could have used the built in SummingCombiner.
    Combiner<String, Long> combiner = input -> input.stream().reduce(Long::sum).filter(l -> l != 0);

    // Called when the value of a key changes. The lambda exports these changes to an external
    // database. Make sure to read ChangeObserver's javadoc.
    ChangeObserver<String, Long> changeObs = (tx, changes) -> {
      for (Change<String, Long> update : changes) {
        String word = update.getKey();
        Optional<Long> oldVal = update.getOldValue();
        Optional<Long> newVal = update.getNewValue();

        // Queue an export to let an external database know the word count has changed.
        exportQ.add(tx, word, new MyDatabaseExport(oldVal, newVal));
      }
    };

    // Register observer that handles updates to the CombineQueue. This observer will use the
    // combiner and valueObserver.
    wcMap.registerObserver(obsRegistry, combiner, changeObs);
  }
}
```

## Guarantees

This recipe makes two important guarantees about updates for a key when it
calls `process()` on a [ChangeObserver].

 * The new value reported for an update will be derived from combining all
   updates that were committed before the transaction thats processing updates
   started.  The implementation may have to make multiple passes over queued
   updates to achieve this.  In the situation where TX1 queues a `+1` and later
   TX2 queues a `-1` for the same key, there is no need to worry about only seeing
   the `-1` processed.  A transaction that started processing updates after TX2
   committed would process both.
 * The old value will always be what was reported as the new value in the
   previous transaction that called `ChangeObserver.process()`.

[phrasecount]: https://github.com/fluo-io/phrasecount
[CombineQueue]: /modules/core/src/main/java/org/apache/fluo/recipes/core/combine/CombineQueue.java
[ChangeObserver]: /modules/core/src/main/java/org/apache/fluo/recipes/core/combine/ChangeObserver.java
[Combiner]: /modules/core/src/main/java/org/apache/fluo/recipes/core/combine/Combiner.java
