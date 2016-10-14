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
# Collision Free Map Recipe

## Background

When many transactions are trying to modify the same keys, collisions will occur.
These collisions will cause the transactions to fail and throughput to nose
dive.  For example consider the [phrasecount] example.  In this example many
transactions are processing documents as input.  Each transaction counts the
phrases in its document and then tries to update global phrase counts.  With
each transaction attempting to update many phrase counts, the probability of
two transactions colliding is very high.

## Solution

This recipe provides a reusable solution for the problem of many transactions
updating many keys while avoiding collisions.  As an added bonus, this recipe
also organizes updates into batches for efficiency in order to improve
throughput.

The central idea behind this recipe is that updates to a key are queued up to
be processed by another transaction triggered by weak notifications.  In the
phrase count example transactions processing documents would queue updates,
but would not actually update the counts.  Below is an example of how
transactions would compute phrasecounts using this recipe.

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

A simple implementation of this recipe would be to have an update queue for
each key.  However the implementation does something slightly more complex.
Each update queue is in a bucket and transactions that process updates, process
all of the updates in a bucket.  This allows more efficient processing of
updates for the following reasons :

 * When updates are queued, notifications are made per bucket(instead of per a key).
 * The transaction doing the update can scan the entire bucket reading updates, this avoids a seek for each key being updated.  
 * Also the transaction can request a batch lookup to get the current value of all the keys being updated.
 * Any additional actions taken on update (like adding something to an export queue) can also be batched.
 * Data is organized to make reading exiting values for keys in a bucket more efficient.

Which bucket a key goes to is decided using hash and modulus so that multiple
updates for the same key always go to the same bucket.

The initial number of tablets to create when applying table optimizations can be
controlled by setting the buckets per tablet option when configuring a Collision
Free Map.  For example if you have 20 tablet servers and 1000 buckets and want
2 tablets per tserver initially then set buckets per tablet to 1000/(2*20)=25.

## Example Use

The following code snippets show how to setup and use this recipe for
wordcount.  The first step in using this recipe is to configure it before
initializing Fluo.  When initializing an ID will need to be provided.  This ID
is used in two ways.  First, the ID is used as a row prefix in the table.
Therefore nothing else should use that row range in the table.  Second, the ID
is used in generating configuration keys associated with the instance of the
Collision Free Map.

The following snippet shows how to setup a collision free map.  

```java
  FluoConfiguration fluoConfig = ...;

  int numBuckets = 119;

  WordCountMap.configure(fluoConfig, 119);

  //initialize Fluo using fluoConfig

```

Assume the following observer is triggered when a documents contents are
updated.  It examines new and old document content and determines changes in
word counts.  These changes are pushed to a collision free map.

```java
public class DocumentObserver extends TypedObserver {

  CollisionFreeMap<String, Long> wcm;

  @Override
  public void init(Context context) throws Exception {
    wcm = CollisionFreeMap.getInstance(WordCountMap.ID, context.getAppConfiguration());
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("content", "new"), NotificationType.STRONG);
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    String newContent = tx.get().row(row).col(col).toString();
    String currentContent = tx.get().row(row).fam("content").qual("current").toString("");

    Map<String, Long> newWordCounts = getWordCounts(newContent);
    Map<String, Long> currentWordCounts = getWordCounts(currentContent);

    //determine changes in word counts between old and new document content
    Map<String, Long> changes = calculateChanges(newWordCounts, currentWordCounts);    

    //queue updates to word counts for processing by other transactions
    wcm.update(tx, changes);

    //update the current content and delete the new content
    tx.mutate().row(row).fam("content").qual("current").set(newContent);
    tx.mutate().row(row).col(col).delete();
  }

  private static Map<String, Long> getWordCounts(String doc) {
   //TODO extract words from doc
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

Each collision free map has two extension points, a combiner and an update
observer.  These two extension points are defined below as `WordCountCombiner`
and  `WordCountObserver`.  The collision free map configures a Fluo observer that
will process queued updates.  When processing these queued updates the two
extension points are called.  In this example `WordCountCombiner` is called to
combine updates that were queued by `DocumentObserver`. The collision free map
will process a batch of keys, calling the combiner for each key.  When finished
processing a batch, it will call the update observer `WordCountObserver`.

An update observer can do additional processing when a batch of key values are
updated.  In `WordCountObserver`, updates are queued for export to an external
database.  The export is given the new and old value allowing it to delete the
old value if needed.

```java
/**
 * This class exists to provide a single place to put all code related to the
 * word count map.
 */
public class WordCountMap {

  public static final String ID = "wc";

  /**
   * A helper method for configuring the word count map.
   *
   * @param numTablets the desired number of tablets to create when applying table optimizations
   */
  public static void configure(FluoConfiguration fluoConfig, int numBuckets, int numTablets) {
    Options cfmOpts =
      new Options(ID, WordCountCombiner.class,  WordCountObserver.class, String.class, Long.class, numBuckets)
        .setBucketsPerTablet(numBuckets/numTablets);
    CollisionFreeMap.configure(fluoConfig, cfmOpts);
  }

  public static class WordCountCombiner implements Combiner<String, Long> {
    @Override
    public Optional<Long> combine(String key, Iterator<Long> updates) {
      long sum = 0L;

      while (updates.hasNext()) {
        sum += updates.next();
      }

      if (sum == 0) {
        //returning absent will cause the collision free map to delte the current key
        return Optional.absent();
      } else {
        return Optional.of(sum);
      }
    }
  }

  public static class WordCountObserver extends UpdateObserver<String, Long> {

    private ExportQueue<String, MyDatabaseExport> exportQ;

    @Override
    public void init(String mapId, Context observerContext) throws Exception {
      exportQ = ExportQueue.getInstance(MyExportQ.ID, observerContext.getAppConfiguration());
    }

    @Override
    public void updatingValues(TransactionBase tx, Iterator<Update<String, Long>> updates) {
      while (updates.hasNext()) {
        Update<String, Long> update = updates.next();

        String word = update.getKey();
        Optional<Long> oldVal = update.getOldValue();
        Optional<Long> newVal = update.getNewValue();

        //queue an export to let an external database know the word count has changed
        exportQ.add(word, new MyDatabaseExport(oldVal, newVal));
      }
    }
  }
}
```

## Guarantees

This recipe makes two important guarantees about updates for a key when it
calls `updatingValues()` on an `UpdateObserver`.

 * The new value reported for an update will be derived from combining all
   updates that were committed before the transaction thats processing updates
   started.  The implementation may have to make multiple passes over queued
   updates to achieve this.  In the situation where TX1 queues a `+1` and later
   TX2 queues a `-1` for the same key, there is no need to worry about only seeing
   the `-1` processed.  A transaction that started processing updates after TX2
   committed would process both.
 * The old value will always be what was reported as the new value in the
   previous transaction that called `updatingValues()`.  
 
[phrasecount]: https://github.com/fluo-io/phrasecount
