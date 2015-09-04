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

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.recipes.serialization.SimpleSerializer;

public abstract class Exporter<K, V> extends AbstractObserver {

  private String queueId;
  private SimpleSerializer<K> keySerializer;
  private SimpleSerializer<V> valueSerializer;

  protected Exporter(String queueId, SimpleSerializer<K> keySerializer,
      SimpleSerializer<V> valueSerializer) {
    this.queueId = queueId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  protected String getQueueId() {
    return queueId;
  }

  SimpleSerializer<K> getKeySerializer() {
    return keySerializer;
  }

  SimpleSerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(Bucket.newNotificationColumn(getQueueId()), NotificationType.WEAK);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column column) throws Exception {
    Bucket bucket = new Bucket(tx, row);

    Iterator<ExportEntry> exportIterator = bucket.getExportIterator();

    startingToProcessBatch();

    while (exportIterator.hasNext()) {
      ExportEntry ee = exportIterator.next();
      processExport(keySerializer.deserialize(ee.key), ee.seq,
          valueSerializer.deserialize(ee.value));
      exportIterator.remove();
    }

    finishedProcessingBatch();
  }

  protected void startingToProcessBatch() {}

  /**
   * Must be able to handle same key being exported multiple times and key being exported out of
   * order. The sequence number is meant to help with this.
   */
  protected abstract void processExport(K key, long sequenceNumber, V value);

  protected void finishedProcessingBatch() {}

  /**
   * Can call in the init method of an observer
   */
  public ExportQueue<K, V> getExportQueue(Configuration appConfig) {
    return new ExportQueue<K, V>(appConfig, this);
  }

  public void setConfiguration(Configuration appConfig, ExportQueueOptions opts) {
    appConfig.setProperty("recipes.exportQueue." + getQueueId() + ".buckets", opts.numBuckets + "");
    appConfig.setProperty("recipes.exportQueue." + getQueueId() + ".counters", opts.numCounters
        + "");
  }
}
