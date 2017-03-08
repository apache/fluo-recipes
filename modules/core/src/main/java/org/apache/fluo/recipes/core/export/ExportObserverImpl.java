/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.recipes.core.export;

import java.util.Iterator;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

import com.google.common.collect.Iterators;

// This class intentionally package private.
class ExportObserverImpl<K, V> implements Observer {

  private String queueId;
  private Class<K> keyType;
  private Class<V> valType;
  SimpleSerializer serializer;
  private org.apache.fluo.recipes.core.export.function.Exporter<K, V> exporter;
  private long memLimit;

  @SuppressWarnings("unchecked")
  ExportObserverImpl(String queueId, FluentConfigurator opts, SimpleSerializer serializer,
      org.apache.fluo.recipes.core.export.function.Exporter<K, V> exportConsumer) throws Exception {
    this.queueId = queueId;

    // TODO move class loading to centralized place... also attempt to check type params
    keyType = (Class<K>) getClass().getClassLoader().loadClass(opts.keyType);
    valType = (Class<V>) getClass().getClassLoader().loadClass(opts.valueType);
    exporter = exportConsumer;

    this.serializer = serializer;

    memLimit = opts.getBufferSize();
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    ExportBucket bucket = new ExportBucket(tx, row);

    Bytes continueRow = bucket.getContinueRow();

    Iterator<ExportEntry> input = bucket.getExportIterator(continueRow);
    MemLimitIterator memLimitIter = new MemLimitIterator(input, memLimit, 8 + queueId.length());

    Iterator<SequencedExport<K, V>> exportIterator =
        Iterators.transform(
            memLimitIter,
            ee -> new SequencedExport<>(serializer.deserialize(ee.key, keyType), serializer
                .deserialize(ee.value, valType), ee.seq));

    exportIterator = Iterators.consumingIterator(exportIterator);

    exporter.export(exportIterator);

    if (input.hasNext() || continueRow != null) {
      // not everything was processed so notify self OR new data may have been inserted above the
      // continue row
      bucket.notifyExportObserver();
    }

    if (input.hasNext()) {
      if (!memLimitIter.hasNext()) {
        // stopped because of mem limit... set continue key
        bucket.setContinueRow(input.next());
        continueRow = null;
      }
    }

    if (continueRow != null) {
      bucket.clearContinueRow();
    }
  }

}
