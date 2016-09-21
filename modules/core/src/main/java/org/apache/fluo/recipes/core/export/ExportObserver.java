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
import java.util.NoSuchElementException;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * @since 1.0.0
 */
public class ExportObserver<K, V> extends AbstractObserver {

  private static class MemLimitIterator implements Iterator<ExportEntry> {

    private long memConsumed = 0;
    private long memLimit;
    private int extraPerKey;
    private Iterator<ExportEntry> source;

    public MemLimitIterator(Iterator<ExportEntry> input, long limit, int extraPerKey) {
      this.source = input;
      this.memLimit = limit;
      this.extraPerKey = extraPerKey;
    }

    @Override
    public boolean hasNext() {
      return memConsumed < memLimit && source.hasNext();
    }

    @Override
    public ExportEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ExportEntry ee = source.next();
      memConsumed += ee.key.length + extraPerKey + ee.value.length;
      return ee;
    }

    @Override
    public void remove() {
      source.remove();
    }
  }

  private String queueId;
  private Class<K> keyType;
  private Class<V> valType;
  SimpleSerializer serializer;
  private Exporter<K, V> exporter;

  private long memLimit;

  protected String getQueueId() {
    return queueId;
  }

  SimpleSerializer getSerializer() {
    return serializer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Context context) throws Exception {
    queueId = context.getObserverConfiguration().getString("queueId");
    ExportQueue.Options opts = new ExportQueue.Options(queueId, context.getAppConfiguration());

    // TODO defer loading classes... so that not done during fluo init
    // TODO move class loading to centralized place... also attempt to check type params
    keyType = (Class<K>) getClass().getClassLoader().loadClass(opts.keyType);
    valType = (Class<V>) getClass().getClassLoader().loadClass(opts.valueType);
    exporter =
        getClass().getClassLoader().loadClass(opts.exporterType).asSubclass(Exporter.class)
            .newInstance();

    serializer = SimpleSerializer.getInstance(context.getAppConfiguration());

    memLimit = opts.getBufferSize();

    exporter.init(new Exporter.Context() {

      @Override
      public String getQueueId() {
        return queueId;
      }

      @Override
      public SimpleConfiguration getExporterConfiguration() {
        return opts.getExporterConfiguration();
      }

      @Override
      public Context getObserverContext() {
        return context;
      }
    });
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(ExportBucket.newNotificationColumn(queueId), NotificationType.WEAK);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column column) throws Exception {
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

    exporter.processExports(exportIterator);

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
