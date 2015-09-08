/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.recipes.export;

import org.apache.commons.configuration.Configuration;

import com.google.common.hash.Hashing;

import io.fluo.api.client.TransactionBase;

public class ExportQueue<K, V> {

  private int numBuckets;
  private int numCounters;
  private Exporter<K, V> exporter;

  // usage hint : could be created once in an observers init method
  // usage hint : maybe have a queue for each type of data being exported???
  // maybe less queues are
  // more efficient though because more batching at export time??
  ExportQueue(Configuration appConfig, Exporter<K, V> exporter) {
    this.numBuckets = appConfig.getInt("recipes.exportQueue." + exporter.getQueueId() + ".buckets");
    if (numBuckets <= 0) {
      throw new IllegalArgumentException("buckets is not positive");
    }

    this.numCounters =
        appConfig.getInt("recipes.exportQueue." + exporter.getQueueId() + ".counters");

    if (numCounters <= 0) {
      throw new IllegalArgumentException("counters is not positive");
    }

    this.exporter = exporter;
  }

  public void add(TransactionBase tx, K key, V value) {

    byte[] k = exporter.getSerializer().serialize(key);
    byte[] v = exporter.getSerializer().serialize(value);

    int hash = Hashing.murmur3_32().hashBytes(k).asInt();
    int bucketId = Math.abs(hash % numBuckets);
    // hash the hash for the case where numBuckets == numCounters... w/o
    // hashing the hash there
    // would only be 1 counter per bucket in this case
    int counter = Math.abs(Hashing.murmur3_32().hashInt(hash).asInt() % numCounters);

    ExportBucket bucket = new ExportBucket(tx, exporter.getQueueId(), bucketId);

    long seq = bucket.getSequenceNumber(counter);

    bucket.add(seq, k, v);

    bucket.setSequenceNumber(counter, seq + 1);

    bucket.notifyExportObserver(k);
  }
}
